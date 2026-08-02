#!/usr/bin/env python3
"""
vps-backup - Sauvegarde complete d'un VPS Debian vers S3 (OVH Object Storage),
en streaming, sans jamais creer d'archive complete sur le disque local.

Toutes les fonctionnalites du projet modulaire d'origine sont conservees,
regroupees ici en un seul fichier autonome (avec restore.py) :
    - pipeline tar | pigz lu par blocs, envoye en Multipart Upload S3
    - reprise automatique apres interruption (reseau coupe, process tue),
      avec S3 comme source de verite (ListParts) et protection contre les
      trous dans la sequence de parties (voir _reconcile_with_s3)
    - verrou anti-execution-concurrente avec detection des verrous obsoletes
    - manifest JSON (SHA256, taille, parties) uploade a cote de chaque backup
    - rotation automatique (conserve les N dernieres sauvegardes)
    - configuration exclusivement via config.toml
    - logs detailles (fichier + console)

Sauvegarde CIBLEE (pas tout le systeme) : au lieu de tar de "/", les sources
sont resolues depuis config.toml a partir de deux listes :
    - backup.include_paths  : chemins fixes (ex: /opt/wordpress, /etc/ufw)
    - backup.docker_volumes : noms de volumes Docker nommes, resolus vers
      leur emplacement reel sur disque via `docker volume inspect`
Un chemin absent au moment du backup est ignore avec un avertissement (ex:
fail2ban non installe) plutot que de faire echouer toute la sauvegarde. Un
inventaire des paquets apt installes (dpkg --get-selections) est genere et
inclus automatiquement a chaque run, pour pouvoir reinstaller les memes
paquets sur un VPS neuf sans avoir a copier leurs binaires.

Notifications email (optionnelles) : si [email] enabled = true dans
config.toml, un email est envoye a la fin de `backup` en cas de succes
(recap : cle, taille, duree, SHA256) et en cas d'echec (message d'erreur).
Credentials SMTP lus depuis config.toml, jamais codes en dur dans le script.

Commandes :
    python3 backup.py backup      [--config config.toml] [--source /chemin]
    python3 backup.py rotate      [--config config.toml] [--dry-run]
    python3 backup.py check       [--config config.toml]
    python3 backup.py test-email  [--config config.toml]
    python3 backup.py version

`--source` est optionnel : s'il est omis, les sources sont resolues depuis
config.toml (comportement normal). Ne l'utilise que pour un backup ponctuel
d'un seul chemin precis.

Voir restore.py pour la restauration et le listing des sauvegardes.
"""
from __future__ import annotations

import hashlib
import json
import logging
import os
import shutil
import smtplib
import socket
import subprocess
import sys
import tempfile
import threading
import time
import tomllib
from concurrent.futures import Future, ThreadPoolExecutor
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from email.mime.text import MIMEText
from pathlib import Path
from typing import Any, Optional

import boto3
import typer
from botocore.client import Config as BotoConfig
from botocore.exceptions import ClientError
from rich.console import Console
from rich.logging import RichHandler
from rich.table import Table

__version__ = "1.0.0"

# ============================================================================
# UTILS
# ============================================================================


class StreamHasher:
    """SHA256 incremental sur un flux d'octets."""

    def __init__(self) -> None:
        self._hash = hashlib.sha256()

    def update(self, chunk: bytes) -> None:
        self._hash.update(chunk)

    def hexdigest(self) -> str:
        return self._hash.hexdigest()


def human_size(num_bytes: float) -> str:
    units = ["o", "Ko", "Mo", "Go", "To", "Po"]
    size = float(num_bytes)
    for unit in units:
        if size < 1024 or unit == units[-1]:
            return f"{size:.2f} {unit}"
        size /= 1024
    return f"{size:.2f} Po"


def which_or_raise(binary: str) -> str:
    path = shutil.which(binary)
    if path is None:
        raise FileNotFoundError(
            f"Binaire requis introuvable dans le PATH : '{binary}'. "
            f"Installe-le (ex: apt install {binary}) avant de continuer."
        )
    return path


def atomic_write_json(path: Path, data: dict[str, Any]) -> None:
    """Ecriture atomique (fichier temporaire + rename) pour ne jamais laisser
    un fichier d'etat corrompu si le process est tue en plein milieu."""
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp_name = tempfile.mkstemp(dir=str(path.parent), prefix=f".{path.name}.", suffix=".tmp")
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp_name, path)
    except Exception:
        Path(tmp_name).unlink(missing_ok=True)
        raise


def read_json(path: Path) -> Optional[dict[str, Any]]:
    if not path.exists():
        return None
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError):
        return None


def free_disk_space(path: Path) -> int:
    target = path if path.exists() else path.parent
    return shutil.disk_usage(target).free


def check_writable(path: Path) -> bool:
    try:
        path.mkdir(parents=True, exist_ok=True)
        test_file = path / ".vps-backup-write-test"
        test_file.write_text("test")
        test_file.unlink(missing_ok=True)
        return True
    except OSError:
        return False


# ============================================================================
# CONFIGURATION (TOML)
# ============================================================================


class ConfigError(Exception):
    """Configuration invalide ou fichier introuvable."""


S3_MIN_PART_MB = 5
S3_MAX_PART_MB = 5 * 1024


@dataclass
class S3Config:
    bucket: str
    endpoint: str
    region: str


@dataclass
class BackupConfig:
    retention: int = 4
    compression_level: int = 3
    part_size_mb: int = 128
    buffer_mb: int = 128
    one_file_system: bool = True
    exclude: list[str] = field(default_factory=list)
    # Sauvegarde ciblee : chemins fixes + volumes Docker nommes (resolus a
    # l'execution). Remplace l'ancienne approche "tout / sauf exclusions".
    include_paths: list[str] = field(default_factory=list)
    docker_volumes: list[str] = field(default_factory=list)


@dataclass
class UploadConfig:
    threads: int = 4
    retries: int = 5
    retry_backoff_seconds: float = 5.0


@dataclass
class PathsConfig:
    log_file: str = "logs/vps-backup.log"
    tmp_dir: str = "state"
    prefix: str = "vps-debian/"


@dataclass
class EmailConfig:
    """Notifications email (optionnelles). Desactivees par defaut : une
    config.toml existante sans section [email] continue de fonctionner
    sans rien envoyer."""
    enabled: bool = False
    smtp_server: str = ""
    smtp_port: int = 587
    address: str = ""
    password: str = ""
    recipients: list[str] = field(default_factory=list)
    retries: int = 3
    retry_delay_seconds: float = 5.0


@dataclass
class Config:
    s3: S3Config
    backup: BackupConfig
    upload: UploadConfig
    paths: PathsConfig
    email: EmailConfig
    base_dir: Path

    @property
    def log_file(self) -> Path:
        return self._resolve(self.paths.log_file)

    @property
    def state_dir(self) -> Path:
        return self._resolve(self.paths.tmp_dir)

    def _resolve(self, value: str) -> Path:
        p = Path(value)
        return p if p.is_absolute() else self.base_dir / p


def load_config(path: str | Path) -> Config:
    config_path = Path(path)
    if not config_path.exists():
        raise ConfigError(f"Fichier de configuration introuvable : {config_path}")

    with config_path.open("rb") as f:
        raw = tomllib.load(f)

    try:
        s3_raw = raw["s3"]
        s3_cfg = S3Config(bucket=s3_raw["bucket"], endpoint=s3_raw["endpoint"], region=s3_raw["region"])
    except KeyError as exc:
        raise ConfigError(f"Section [s3] incomplete, cle manquante : {exc}") from exc

    b = raw.get("backup", {})
    backup_cfg = BackupConfig(
        retention=int(b.get("retention", 4)),
        compression_level=int(b.get("compression_level", 3)),
        part_size_mb=int(b.get("part_size_mb", 128)),
        buffer_mb=int(b.get("buffer_mb", 128)),
        one_file_system=bool(b.get("one_file_system", True)),
        exclude=list(b.get("exclude", [])),
        include_paths=list(b.get("include_paths", [])),
        docker_volumes=list(b.get("docker_volumes", [])),
    )

    u = raw.get("upload", {})
    upload_cfg = UploadConfig(
        threads=int(u.get("threads", 4)),
        retries=int(u.get("retries", 5)),
        retry_backoff_seconds=float(u.get("retry_backoff_seconds", 5.0)),
    )

    p = raw.get("paths", {})
    paths_cfg = PathsConfig(
        log_file=p.get("log_file", "logs/vps-backup.log"),
        tmp_dir=p.get("tmp_dir", "state"),
        prefix=p.get("prefix", "vps-debian/"),
    )

    e = raw.get("email", {})
    email_cfg = EmailConfig(
        enabled=bool(e.get("enabled", False)),
        smtp_server=e.get("smtp_server", ""),
        smtp_port=int(e.get("smtp_port", 587)),
        address=e.get("address", ""),
        password=e.get("password", ""),
        recipients=list(e.get("recipients", [])),
        retries=int(e.get("retries", 3)),
        retry_delay_seconds=float(e.get("retry_delay_seconds", 5.0)),
    )

    cfg = Config(s3=s3_cfg, backup=backup_cfg, upload=upload_cfg, paths=paths_cfg, email=email_cfg, base_dir=config_path.resolve().parent)
    _validate(cfg)
    return cfg


def _validate(cfg: Config) -> None:
    errors: list[str] = []
    if not cfg.s3.bucket:
        errors.append("s3.bucket ne peut pas etre vide")
    if not cfg.s3.endpoint.startswith("https://"):
        errors.append("s3.endpoint doit commencer par https://")
    if not cfg.s3.region:
        errors.append("s3.region ne peut pas etre vide")
    if cfg.backup.retention < 1:
        errors.append("backup.retention doit etre >= 1")
    if not (1 <= cfg.backup.compression_level <= 9):
        errors.append("backup.compression_level doit etre entre 1 et 9")
    if not (S3_MIN_PART_MB <= cfg.backup.part_size_mb <= S3_MAX_PART_MB):
        errors.append(f"backup.part_size_mb doit etre entre {S3_MIN_PART_MB} et {S3_MAX_PART_MB}")
    if cfg.backup.buffer_mb < 1:
        errors.append("backup.buffer_mb doit etre >= 1")
    if cfg.upload.threads < 1:
        errors.append("upload.threads doit etre >= 1")
    if cfg.upload.retries < 0:
        errors.append("upload.retries doit etre >= 0")
    if not cfg.backup.include_paths and not cfg.backup.docker_volumes:
        errors.append("backup.include_paths et backup.docker_volumes sont tous les deux vides : rien a sauvegarder")
    if cfg.email.enabled:
        if not cfg.email.smtp_server:
            errors.append("email.smtp_server requis quand email.enabled = true")
        if not cfg.email.address or not cfg.email.password:
            errors.append("email.address et email.password requis quand email.enabled = true")
        if not cfg.email.recipients:
            errors.append("email.recipients ne peut pas etre vide quand email.enabled = true")
    if errors:
        raise ConfigError("Configuration invalide :\n- " + "\n- ".join(errors))


# ============================================================================
# LOGGING
# ============================================================================

_LOGGER_NAME = "vps_backup"


def setup_logger(log_file: Path, verbose: bool = False) -> logging.Logger:
    logger = logging.getLogger(_LOGGER_NAME)
    if logger.handlers:
        return logger
    logger.setLevel(logging.DEBUG if verbose else logging.INFO)

    log_file.parent.mkdir(parents=True, exist_ok=True)
    file_handler = logging.FileHandler(log_file, encoding="utf-8")
    file_handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
    logger.addHandler(file_handler)

    console_handler = RichHandler(show_path=False, rich_tracebacks=True, markup=False)
    console_handler.setFormatter(logging.Formatter("%(message)s"))
    logger.addHandler(console_handler)
    logger.propagate = False
    return logger


logger = logging.getLogger(_LOGGER_NAME)

# ============================================================================
# VERROU (anti-execution-concurrente)
# ============================================================================


class LockError(Exception):
    """Une autre execution de vps-backup est deja en cours."""


def _pid_is_alive(pid: int) -> bool:
    if pid <= 0:
        return False
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    return True


class BackupLock:
    """Verrou base sur un fichier PID, avec detection des verrous obsoletes."""

    def __init__(self, lock_path: Path, command: str = "backup") -> None:
        self.lock_path = lock_path
        self.command = command
        self._acquired = False

    def acquire(self) -> None:
        self.lock_path.parent.mkdir(parents=True, exist_ok=True)
        if self.lock_path.exists():
            existing_pid = -1
            try:
                info = json.loads(self.lock_path.read_text())
                existing_pid = int(info.get("pid", -1))
            except (ValueError, json.JSONDecodeError, OSError):
                pass
            if _pid_is_alive(existing_pid):
                raise LockError(f"Une operation vps-backup est deja en cours (pid={existing_pid}). Verrou : {self.lock_path}")
            logger.warning("Verrou obsolete detecte (pid=%s introuvable) - suppression et poursuite", existing_pid)
            self.lock_path.unlink(missing_ok=True)

        info = {"pid": os.getpid(), "started": time.strftime("%Y-%m-%dT%H:%M:%S"), "command": self.command}
        self.lock_path.write_text(json.dumps(info))
        self._acquired = True

    def release(self) -> None:
        if not self._acquired:
            return
        try:
            info = json.loads(self.lock_path.read_text())
            if int(info.get("pid", -1)) == os.getpid():
                self.lock_path.unlink(missing_ok=True)
        except (ValueError, json.JSONDecodeError, OSError):
            pass
        self._acquired = False

    def __enter__(self) -> "BackupLock":
        self.acquire()
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.release()


# ============================================================================
# PIPELINE tar | pigz (source)
# ============================================================================


class ArchiveError(Exception):
    """Erreur pendant la creation de l'archive (tar ou pigz)."""


class ResumeMismatchError(Exception):
    """Le flux regenere pour la reprise est plus court que prevu : la
    sauvegarde precedente n'est plus reproductible a l'identique (fichiers
    modifies/supprimes entre-temps)."""


@dataclass
class SourcePipeline:
    tar_proc: subprocess.Popen
    pigz_proc: subprocess.Popen

    def read(self, size: int) -> bytes:
        return self.pigz_proc.stdout.read(size)  # type: ignore[union-attr]

    def close(self) -> tuple[int, int]:
        if self.pigz_proc.stdout:
            self.pigz_proc.stdout.close()
        pigz_rc = self.pigz_proc.wait()
        tar_rc = self.tar_proc.wait()
        return tar_rc, pigz_rc

    def tar_stderr(self) -> str:
        return self.tar_proc.stderr.read().decode(errors="replace") if self.tar_proc.stderr else ""

    def pigz_stderr(self) -> str:
        return self.pigz_proc.stderr.read().decode(errors="replace") if self.pigz_proc.stderr else ""


def build_source_pipeline(cfg: BackupConfig, sources: list[str]) -> SourcePipeline:
    """Demarre `tar | pigz` sur une LISTE de chemins precis (pas tout le
    disque) : le flux compresse resultant est lu par blocs, jamais stocke
    entierement sur disque."""
    if not sources:
        raise ArchiveError("Aucune source a sauvegarder (liste vide)")
    which_or_raise("tar")
    which_or_raise("pigz")

    tar_cmd = ["tar", "-cpf", "-"]
    if cfg.one_file_system:
        tar_cmd.append("--one-file-system")
    for excl in cfg.exclude:
        tar_cmd += ["--exclude", excl]
    tar_cmd.extend(sources)

    tar_proc = subprocess.Popen(tar_cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    pigz_cmd = ["pigz", f"-{cfg.compression_level}", "-c"]
    pigz_proc = subprocess.Popen(pigz_cmd, stdin=tar_proc.stdout, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    if tar_proc.stdout:
        tar_proc.stdout.close()  # laisse tar recevoir SIGPIPE si pigz s'arrete
    return SourcePipeline(tar_proc=tar_proc, pigz_proc=pigz_proc)


def resolve_docker_volume_mountpoint(volume_name: str) -> str:
    """Resout un volume Docker nomme vers son emplacement reel sur le disque
    via `docker volume inspect` (plus fiable qu'une supposition sur le
    chemin par defaut de Docker, qui peut etre personnalise)."""
    which_or_raise("docker")
    result = subprocess.run(
        ["docker", "volume", "inspect", volume_name, "--format", "{{ .Mountpoint }}"],
        capture_output=True, text=True,
    )
    if result.returncode != 0:
        raise FileNotFoundError(f"Volume Docker introuvable : '{volume_name}' ({result.stderr.strip()})")
    return result.stdout.strip()


def generate_package_list(cfg: Config) -> Optional[Path]:
    """Genere un inventaire des paquets apt installes, inclus automatiquement
    dans chaque sauvegarde. Restauration sur un VPS neuf :
        dpkg --set-selections < packages.list && apt-get dselect-upgrade
    N'interrompt pas la sauvegarde si dpkg est indisponible (log un warning)."""
    try:
        output_path = cfg.state_dir / "packages.list"
        output_path.parent.mkdir(parents=True, exist_ok=True)
        result = subprocess.run(["dpkg", "--get-selections"], capture_output=True, text=True, check=True)
        output_path.write_text(result.stdout)
        return output_path
    except (OSError, subprocess.CalledProcessError) as exc:
        logger.warning("Impossible de generer l'inventaire des paquets (dpkg) : %s", exc)
        return None


def resolve_backup_sources(cfg: Config) -> list[str]:
    """Construit la liste finale des chemins a sauvegarder : chemins fixes de
    la config + volumes Docker resolus + inventaire des paquets genere a la
    volee. Les chemins absents sont ignores avec un avertissement plutot que
    de faire echouer toute la sauvegarde (ex: fail2ban non installe)."""
    candidates: list[str] = list(cfg.backup.include_paths)

    for volume_name in cfg.backup.docker_volumes:
        try:
            candidates.append(resolve_docker_volume_mountpoint(volume_name))
        except FileNotFoundError as exc:
            logger.warning("%s - ignore pour cette sauvegarde", exc)

    package_list = generate_package_list(cfg)
    if package_list is not None:
        candidates.append(str(package_list))

    resolved: list[str] = []
    for path_str in candidates:
        if Path(path_str).exists():
            resolved.append(path_str)
        else:
            logger.warning("Chemin configure introuvable, ignore : %s", path_str)

    if not resolved:
        raise ArchiveError("Aucun chemin a sauvegarder n'existe reellement - verifie config.toml")

    logger.info("Sources resolues pour cette sauvegarde : %s", ", ".join(resolved))
    return resolved


def skip_bytes(pipeline: SourcePipeline, count: int, buffer_size: int, hasher: Optional[StreamHasher] = None) -> None:
    """Jette les `count` premiers octets du flux regenere (reprise). Cout CPU
    local (recompression) contre economie de bande passante reseau."""
    remaining = count
    while remaining > 0:
        chunk = pipeline.read(min(buffer_size, remaining))
        if not chunk:
            raise ResumeMismatchError(
                "Le flux regenere est plus court que la portion deja envoyee : "
                "la sauvegarde precedente n'est plus reproductible a l'identique."
            )
        if hasher is not None:
            hasher.update(chunk)
        remaining -= len(chunk)


# ============================================================================
# CLIENT S3 (boto3) - Multipart Upload exclusivement pour le contenu
# ============================================================================


def build_client(cfg: S3Config):
    return boto3.client(
        "s3",
        endpoint_url=cfg.endpoint,
        region_name=cfg.region,
        config=BotoConfig(signature_version="s3v4", retries={"max_attempts": 0}),
    )


def head_bucket(client, bucket: str) -> bool:
    try:
        client.head_bucket(Bucket=bucket)
        return True
    except ClientError:
        return False


def create_multipart_upload(client, bucket: str, key: str) -> str:
    return client.create_multipart_upload(Bucket=bucket, Key=key)["UploadId"]


def upload_part(client, bucket: str, key: str, upload_id: str, part_number: int, body: bytes) -> str:
    return client.upload_part(Bucket=bucket, Key=key, UploadId=upload_id, PartNumber=part_number, Body=body)["ETag"]


def list_parts(client, bucket: str, key: str, upload_id: str) -> list[dict[str, Any]]:
    parts: list[dict[str, Any]] = []
    kwargs: dict[str, Any] = {"Bucket": bucket, "Key": key, "UploadId": upload_id}
    while True:
        response = client.list_parts(**kwargs)
        parts.extend(response.get("Parts", []))
        if not response.get("IsTruncated"):
            break
        kwargs["PartNumberMarker"] = response["NextPartNumberMarker"]
    return parts


def complete_multipart_upload(client, bucket: str, key: str, upload_id: str, parts: list[dict[str, Any]]) -> dict:
    return client.complete_multipart_upload(Bucket=bucket, Key=key, UploadId=upload_id, MultipartUpload={"Parts": parts})


def put_object_bytes(client, bucket: str, key: str, data: bytes) -> None:
    """Reserve aux petits objets annexes (manifests JSON) - jamais pour le
    contenu de la sauvegarde elle-meme."""
    client.put_object(Bucket=bucket, Key=key, Body=data)


def list_objects(client, bucket: str, prefix: str) -> list[dict[str, Any]]:
    objects: list[dict[str, Any]] = []
    paginator = client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        objects.extend(page.get("Contents", []))
    return objects


def delete_object(client, bucket: str, key: str) -> None:
    client.delete_object(Bucket=bucket, Key=key)


def get_object_bytes(client, bucket: str, key: str) -> bytes:
    return client.get_object(Bucket=bucket, Key=key)["Body"].read()


# ============================================================================
# MANIFEST
# ============================================================================


@dataclass
class PartInfo:
    part_number: int
    size: int
    sha256: str
    etag: str


@dataclass
class Manifest:
    hostname: str
    created: str
    compression: str
    compression_level: int
    part_size_mb: int
    total_size: int
    sha256: str
    backup_key: str
    upload_id: str
    parts: list[PartInfo] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> "Manifest":
        parts = [PartInfo(**p) for p in d.get("parts", [])]
        return cls(
            hostname=d["hostname"], created=d["created"], compression=d["compression"],
            compression_level=d["compression_level"], part_size_mb=d["part_size_mb"],
            total_size=d["total_size"], sha256=d["sha256"], backup_key=d["backup_key"],
            upload_id=d.get("upload_id", ""), parts=parts,
        )


def manifest_key_for(backup_key: str) -> str:
    return f"{backup_key}.manifest.json"


def upload_manifest(manifest_obj: Manifest, client, bucket: str) -> None:
    data = json.dumps(manifest_obj.to_dict(), indent=2).encode("utf-8")
    put_object_bytes(client, bucket, manifest_key_for(manifest_obj.backup_key), data)


def download_manifest(client, bucket: str, manifest_key: str) -> Manifest:
    return Manifest.from_dict(json.loads(get_object_bytes(client, bucket, manifest_key)))


def list_all_manifests(client, bucket: str, prefix: str) -> list[Manifest]:
    manifests: list[Manifest] = []
    for obj in list_objects(client, bucket, prefix):
        if obj["Key"].endswith(".manifest.json"):
            try:
                manifests.append(download_manifest(client, bucket, obj["Key"]))
            except (json.JSONDecodeError, KeyError):
                continue
    return sorted(manifests, key=lambda m: m.created)


# ============================================================================
# ORCHESTRATION MULTIPART (coeur : upload resumable sans archive locale)
# ============================================================================

_RESUME_FILE = "resume.json"


class UploadError(Exception):
    """Echec definitif de l'envoi d'une partie apres epuisement des tentatives."""


@dataclass
class ResumeState:
    key: str
    upload_id: str
    started: str
    parts: dict[int, PartInfo] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "key": self.key, "upload_id": self.upload_id, "started": self.started,
            "parts": {str(n): asdict(p) for n, p in self.parts.items()},
        }

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> "ResumeState":
        parts = {int(n): PartInfo(**p) for n, p in d.get("parts", {}).items()}
        return cls(key=d["key"], upload_id=d["upload_id"], started=d["started"], parts=parts)


def _resume_path(cfg: Config) -> Path:
    return cfg.state_dir / _RESUME_FILE


def load_resume_state(cfg: Config) -> Optional[ResumeState]:
    data = read_json(_resume_path(cfg))
    if data is None:
        return None
    try:
        return ResumeState.from_dict(data)
    except (KeyError, ValueError):
        logger.warning("resume.json illisible/corrompu - ignore, nouvelle sauvegarde")
        return None


def save_resume_state(cfg: Config, state: ResumeState) -> None:
    atomic_write_json(_resume_path(cfg), state.to_dict())


def clear_resume_state(cfg: Config) -> None:
    _resume_path(cfg).unlink(missing_ok=True)


def generate_backup_key(cfg: Config) -> str:
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%S")
    return f"{cfg.paths.prefix}vps-backup-{timestamp}.tar.gz"


@dataclass
class UploadResult:
    key: str
    upload_id: str
    total_size: int
    sha256: str
    parts: list[PartInfo]


def _upload_part_with_retry(client, cfg: Config, key: str, upload_id: str, part_number: int, body: bytes) -> str:
    last_exc: Optional[Exception] = None
    for attempt in range(1, cfg.upload.retries + 2):
        try:
            return upload_part(client, cfg.s3.bucket, key, upload_id, part_number, body)
        except ClientError as exc:
            last_exc = exc
            if attempt > cfg.upload.retries:
                break
            wait = cfg.upload.retry_backoff_seconds * attempt
            logger.warning("Retry %d/%d pour la partie %d (%s) - nouvelle tentative dans %.0fs", attempt, cfg.upload.retries, part_number, exc, wait)
            time.sleep(wait)
    raise UploadError(f"Echec definitif de l'envoi de la partie {part_number}") from last_exc


def _read_part(pipeline: SourcePipeline, part_size_mb: int, buffer_mb: int, hasher: StreamHasher) -> bytes:
    target = part_size_mb * 1024 * 1024
    buf_size = buffer_mb * 1024 * 1024
    chunks: list[bytes] = []
    collected = 0
    while collected < target:
        chunk = pipeline.read(min(buf_size, target - collected))
        if not chunk:
            break
        chunks.append(chunk)
        collected += len(chunk)
        hasher.update(chunk)
    return b"".join(chunks)


def _reconcile_with_s3(client, cfg: Config, key: str, upload_id: str, local_parts: dict[int, PartInfo]) -> dict[int, PartInfo]:
    """S3 est la source de verite. IMPORTANT : on ne retient que le plus long
    prefixe CONTIGU de numeros de partie a partir de 1. Avec des uploads en
    parallele (threads > 1), une partie numerotee plus haut peut reussir sur
    S3 alors qu'une partie intermediaire a echoue : la considerer comme
    "acquise" romprait l'hypothese (skip = somme des tailles = prefixe
    ininterrompu du flux) et produirait une archive corrompue au milieu. Les
    parties au-dela du premier trou sont ignorees et seront simplement
    ecrasees par un nouvel upload_part() au meme numero lors de la reprise.
    (Bug reel detecte et corrige via test avec panne reseau simulee.)
    """
    remote_by_number = {p["PartNumber"]: p for p in list_parts(client, cfg.s3.bucket, key, upload_id)}
    reconciled: dict[int, PartInfo] = {}
    number = 1
    while number in remote_by_number:
        rp = remote_by_number[number]
        local = local_parts.get(number)
        sha = local.sha256 if (local and local.size == rp["Size"]) else "inconnu-reconcilie-depuis-s3"
        reconciled[number] = PartInfo(part_number=number, size=rp["Size"], sha256=sha, etag=rp["ETag"])
        number += 1
    orphaned = sorted(set(remote_by_number) - set(reconciled))
    if orphaned:
        logger.warning("Parties recues hors sequence contigue, ignorees et seront ecrasees lors de la reprise : %s", orphaned)
    return reconciled


def run_backup_upload(cfg: Config, sources: Optional[list[str]] = None) -> UploadResult:
    """Execute (ou reprend) la sauvegarde en flux vers S3. Ne cree jamais
    d'archive complete locale.

    Si `sources` n'est pas fourni, les chemins sont resolus depuis
    config.toml (backup.include_paths + backup.docker_volumes) : c'est le
    fonctionnement normal. `sources` explicite est reserve a un backup
    ponctuel d'un seul chemin (option --source de la CLI).
    """
    if sources is None:
        sources = resolve_backup_sources(cfg)

    client = build_client(cfg.s3)
    resume_state = load_resume_state(cfg)
    completed_parts: dict[int, PartInfo] = {}

    if resume_state is not None:
        logger.info("Reprise d'une sauvegarde interrompue : %s (upload_id=%s)", resume_state.key, resume_state.upload_id)
        try:
            completed_parts = _reconcile_with_s3(client, cfg, resume_state.key, resume_state.upload_id, resume_state.parts)
            key, upload_id = resume_state.key, resume_state.upload_id
        except ClientError:
            logger.warning("UploadId de reprise invalide ou expire - demarrage d'une nouvelle sauvegarde")
            resume_state = None

    if resume_state is None:
        key = generate_backup_key(cfg)
        upload_id = create_multipart_upload(client, cfg.s3.bucket, key)
        completed_parts = {}
        save_resume_state(cfg, ResumeState(key=key, upload_id=upload_id, started=datetime.now(timezone.utc).isoformat(), parts={}))
        logger.info("Nouvelle sauvegarde : %s (upload_id=%s)", key, upload_id)

    bytes_to_skip = sum(p.size for p in completed_parts.values())
    next_part_number = (max(completed_parts.keys()) + 1) if completed_parts else 1

    logger.info("Compression en cours (pigz, niveau %d)", cfg.backup.compression_level)
    pipeline = build_source_pipeline(cfg.backup, sources=sources)
    hasher = StreamHasher()
    state_lock = threading.Lock()

    def persist_progress() -> None:
        save_resume_state(cfg, ResumeState(key=key, upload_id=upload_id, started=datetime.now(timezone.utc).isoformat(), parts=completed_parts))

    try:
        if bytes_to_skip:
            logger.info("Reconstitution du flux jusqu'a l'octet %d (parties deja confirmees par S3)", bytes_to_skip)
            skip_bytes(pipeline, bytes_to_skip, cfg.backup.buffer_mb * 1024 * 1024, hasher=hasher)

        pending: list[tuple[Future, int, int, str]] = []
        part_number = next_part_number

        def drain_one() -> None:
            fut, pn, size, sha = pending.pop(0)
            etag = fut.result()
            with state_lock:
                completed_parts[pn] = PartInfo(part_number=pn, size=size, sha256=sha, etag=etag)
                persist_progress()
            logger.info("Partie %d confirmee par S3 (%s)", pn, human_size(size))

        with ThreadPoolExecutor(max_workers=cfg.upload.threads) as executor:
            while True:
                chunk = _read_part(pipeline, cfg.backup.part_size_mb, cfg.backup.buffer_mb, hasher)
                if not chunk:
                    break
                sha = hashlib.sha256(chunk).hexdigest()
                logger.info("Envoi de la partie %d (%s)", part_number, human_size(len(chunk)))
                fut = executor.submit(_upload_part_with_retry, client, cfg, key, upload_id, part_number, chunk)
                pending.append((fut, part_number, len(chunk), sha))
                if len(pending) >= cfg.upload.threads * 2:
                    drain_one()
                part_number += 1
            while pending:
                drain_one()

        tar_rc, pigz_rc = pipeline.close()
        if tar_rc not in (0, 1):
            raise ArchiveError(f"tar a echoue (code {tar_rc}) : {pipeline.tar_stderr()[-2000:]}")
        if pigz_rc != 0:
            raise ArchiveError(f"pigz a echoue (code {pigz_rc}) : {pipeline.pigz_stderr()[-2000:]}")

    except Exception:
        logger.exception("Echec pendant l'upload - l'etat de reprise est conserve (resume.json) ; relance la commande pour reprendre automatiquement.")
        raise

    total_size = sum(p.size for p in completed_parts.values())
    parts_sorted = [completed_parts[n] for n in sorted(completed_parts)]
    api_parts = [{"PartNumber": p.part_number, "ETag": p.etag} for p in parts_sorted]

    logger.info("Finalisation du Multipart Upload (%d parties, %s)", len(parts_sorted), human_size(total_size))
    complete_multipart_upload(client, cfg.s3.bucket, key, upload_id, api_parts)
    clear_resume_state(cfg)

    return UploadResult(key=key, upload_id=upload_id, total_size=total_size, sha256=hasher.hexdigest(), parts=parts_sorted)


# ============================================================================
# ROTATION
# ============================================================================


def perform_rotation(cfg: Config, client, dry_run: bool = False) -> list[str]:
    """Supprime les sauvegardes en exces (retention). Jamais appelee avant
    la validation complete de la nouvelle sauvegarde (manifest uploade)."""
    backups = list_all_manifests(client, cfg.s3.bucket, cfg.paths.prefix)
    excess = len(backups) - cfg.backup.retention
    if excess <= 0:
        logger.info("Rotation : %d sauvegarde(s), rien a supprimer (limite %d)", len(backups), cfg.backup.retention)
        return []
    deleted = []
    for m in backups[:excess]:
        logger.info("Rotation : suppression de %s (cree le %s)", m.backup_key, m.created)
        if not dry_run:
            delete_object(client, cfg.s3.bucket, m.backup_key)
            delete_object(client, cfg.s3.bucket, manifest_key_for(m.backup_key))
        deleted.append(m.backup_key)
    return deleted


# ============================================================================
# EMAIL (notifications succes/echec) - adapte du script mail existant,
# credentials lus depuis config.toml, jamais codes en dur ici.
# ============================================================================


def send_mail(cfg: EmailConfig, destinataires: list[str], subject: str, message: str) -> bool:
    """Envoie un email (meme logique que le script mail existant : SMTP +
    STARTTLS + retries), avec les credentials pris dans config.toml."""
    if not destinataires:
        logger.warning("Aucun destinataire configure pour l'email '%s' - envoi ignore", subject)
        return False

    for attempt in range(1, cfg.retries + 1):
        server = None
        try:
            server = smtplib.SMTP(cfg.smtp_server, cfg.smtp_port)
            server.starttls()
            server.login(cfg.address, cfg.password)
            for destinataire in destinataires:
                msg = MIMEText(message, "plain", "utf-8")
                msg["From"] = cfg.address
                msg["To"] = destinataire
                msg["Subject"] = subject
                server.sendmail(cfg.address, destinataire, msg.as_string())
            server.quit()
            return True
        except Exception as exc:
            logger.warning("Erreur d'envoi email (tentative %d/%d) : %s", attempt, cfg.retries, exc)
            if server is not None:
                try:
                    server.quit()
                except Exception:
                    pass
            if attempt < cfg.retries:
                time.sleep(cfg.retry_delay_seconds)
    return False


def notify(cfg: Config, subject: str, message: str) -> None:
    """Envoie une notification email si active en config. N'interrompt
    jamais la sauvegarde si l'envoi echoue - juste un warning en log."""
    if not cfg.email.enabled:
        return
    if send_mail(cfg.email, cfg.email.recipients, subject, message):
        logger.info("Email de notification envoye : %s", subject)
    else:
        logger.warning("Echec de l'envoi de l'email de notification (voir warnings ci-dessus)")


# ============================================================================
# CLI (Typer)
# ============================================================================

app = typer.Typer(add_completion=False, help="Sauvegarde de VPS vers S3 (OVH Object Storage), sans archive locale.")
console = Console()

DEFAULT_CONFIG = Path("config.toml")
ConfigOption = typer.Option(DEFAULT_CONFIG, "--config", "-c", help="Chemin du fichier config.toml")


def _load(config: Path) -> Config:
    try:
        cfg = load_config(config)
    except ConfigError as exc:
        console.print(f"[red]Configuration invalide :[/red] {exc}")
        raise typer.Exit(code=1)
    setup_logger(cfg.log_file)
    return cfg


@app.command()
def backup(
    config: Path = ConfigOption,
    source: Optional[str] = typer.Option(None, "--source", help="Remplace la resolution automatique par un seul chemin precis (usage ponctuel)"),
) -> None:
    """Lance une sauvegarde (streaming, reprise automatique si interrompue).

    Par defaut, les sources sont resolues depuis config.toml (chemins fixes
    + volumes Docker). Utilise --source pour sauvegarder un seul chemin
    precis a la place, sans toucher a la configuration.
    """
    cfg = _load(config)
    lock = BackupLock(cfg.state_dir / "backup.lock", command="backup")
    try:
        with lock:
            logger.info("=== Sauvegarde demarree (%s) ===", f"source={source}" if source else "sources resolues depuis config.toml")
            started = datetime.now(timezone.utc)
            result = run_backup_upload(cfg, sources=[source] if source else None)

            m = Manifest(
                hostname=socket.gethostname(), created=started.strftime("%Y-%m-%dT%H:%M:%SZ"),
                compression="pigz", compression_level=cfg.backup.compression_level,
                part_size_mb=cfg.backup.part_size_mb, total_size=result.total_size, sha256=result.sha256,
                backup_key=result.key, upload_id=result.upload_id, parts=result.parts,
            )
            client = build_client(cfg.s3)
            upload_manifest(m, client, cfg.s3.bucket)
            logger.info("Manifest uploade")

            perform_rotation(cfg, client)
            logger.info("=== Sauvegarde terminee avec succes ===")

        duration = datetime.now(timezone.utc) - started
        console.print(f"[green]Sauvegarde terminee :[/green] {result.key} ({human_size(result.total_size)})")
        notify(
            cfg,
            subject=f"[vps-backup] Sauvegarde reussie - {socket.gethostname()}",
            message=(
                f"Sauvegarde terminee avec succes sur {socket.gethostname()}.\n\n"
                f"Fichier : {result.key}\n"
                f"Taille : {human_size(result.total_size)}\n"
                f"Duree : {duration}\n"
                f"Parties : {len(result.parts)}\n"
                f"SHA256 : {result.sha256}\n"
            ),
        )
    except LockError as exc:
        console.print(f"[red]{exc}[/red]")
        raise typer.Exit(code=1)
    except Exception as exc:
        console.print(f"[red]Echec de la sauvegarde :[/red] {exc}")
        console.print(f"Voir les logs : {cfg.log_file}")
        notify(
            cfg,
            subject=f"[vps-backup] ECHEC de la sauvegarde - {socket.gethostname()}",
            message=(
                f"La sauvegarde a echoue sur {socket.gethostname()}.\n\n"
                f"Erreur : {exc}\n\n"
                f"Consulte les logs pour le detail : {cfg.log_file}\n"
                f"La reprise sera automatique au prochain lancement (state/resume.json)."
            ),
        )
        raise typer.Exit(code=1)


@app.command()
def rotate(
    config: Path = ConfigOption,
    dry_run: bool = typer.Option(False, "--dry-run", help="Simule sans rien supprimer"),
) -> None:
    """Force une passe de rotation manuelle (normalement automatique apres chaque backup)."""
    cfg = _load(config)
    client = build_client(cfg.s3)
    deleted = perform_rotation(cfg, client, dry_run=dry_run)
    if deleted:
        console.print(f"[yellow]{'(dry-run) ' if dry_run else ''}Supprime :[/yellow] {', '.join(deleted)}")
    else:
        console.print("Rien a supprimer.")


@app.command()
def check(config: Path = ConfigOption) -> None:
    """Verifie l'environnement : binaires, connexion S3, permissions, config."""
    table = Table(title="Verification de l'environnement vps-backup")
    table.add_column("Verification")
    table.add_column("Resultat")

    def check_item(label: str, fn) -> None:
        try:
            ok, detail = fn()
        except Exception as exc:  # noqa: BLE001
            ok, detail = False, str(exc)
        status = "[green]OK[/green]" if ok else "[red]ECHEC[/red]"
        table.add_row(label, f"{status} - {detail}" if detail else status)

    check_item("Python >= 3.13", lambda: (sys.version_info >= (3, 13), sys.version.split()[0]))
    check_item("Binaire tar", lambda: (True, which_or_raise("tar")))
    check_item("Binaire pigz", lambda: (True, which_or_raise("pigz")))

    try:
        cfg = load_config(config)
        check_item("Configuration valide", lambda: (True, str(config)))
    except ConfigError as exc:
        check_item("Configuration valide", lambda: (False, str(exc)))
        console.print(table)
        raise typer.Exit(code=1)

    check_item("Acces ecriture (state_dir)", lambda: (check_writable(cfg.state_dir), str(cfg.state_dir)))
    check_item("Acces ecriture (log_file)", lambda: (check_writable(cfg.log_file.parent), str(cfg.log_file.parent)))
    free = free_disk_space(cfg.state_dir)
    check_item("Espace disque local", lambda: (free > 200 * 1024 * 1024, human_size(free) + " libres"))

    def _sources_check():
        sources = resolve_backup_sources(cfg)
        return True, f"{len(sources)} chemin(s) : {', '.join(sources)}"

    check_item("Resolution des sources (chemins + volumes Docker)", _sources_check)

    def _s3_check():
        client = build_client(cfg.s3)
        ok = head_bucket(client, cfg.s3.bucket)
        return ok, cfg.s3.bucket if ok else f"bucket '{cfg.s3.bucket}' inaccessible (endpoint/region/credentials ?)"

    check_item("Connexion S3 + bucket", _s3_check)
    console.print(table)


@app.command("test-email")
def test_email(config: Path = ConfigOption) -> None:
    """Envoie un email de test, pour valider la config SMTP sans lancer de vraie sauvegarde."""
    cfg = _load(config)
    if not cfg.email.enabled:
        console.print("[yellow]email.enabled = false dans config.toml - active-le avant de tester.[/yellow]")
        raise typer.Exit(code=1)

    ok = send_mail(
        cfg.email,
        cfg.email.recipients,
        subject=f"[vps-backup] Email de test - {socket.gethostname()}",
        message=f"Ceci est un email de test envoye par vps-backup depuis {socket.gethostname()}.\nSi tu le reçois, la configuration SMTP fonctionne.",
    )
    if ok:
        console.print(f"[green]Email de test envoye avec succes a :[/green] {', '.join(cfg.email.recipients)}")
    else:
        console.print("[red]Echec de l'envoi - voir les logs pour le detail (identifiants, port, pare-feu sortant...).[/red]")
        raise typer.Exit(code=1)


@app.command()
def version() -> None:
    """Affiche la version de vps-backup."""
    console.print(f"vps-backup {__version__}")


if __name__ == "__main__":
    app()
