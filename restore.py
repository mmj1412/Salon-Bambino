#!/usr/bin/env python3
"""
vps-restore - Restauration et listing des sauvegardes VPS creees par backup.py.

Autonome (aucun import depuis backup.py) : duplique volontairement les
quelques briques necessaires (config, client S3, manifest) pour respecter la
contrainte de n'avoir que deux scripts independants.

Le flux est restaure en streaming (S3 -> pigz -d -> tar x) : aucune archive
temporaire n'est jamais creee sur le disque local.

Commandes :
    python3 restore.py list    [--config config.toml]
    python3 restore.py restore <cle_s3> <dest> [--config config.toml] [--no-verify]
"""
from __future__ import annotations

import hashlib
import json
import logging
import subprocess
import tomllib
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any, Optional

import boto3
import typer
from botocore.client import Config as BotoConfig
from rich.console import Console
from rich.logging import RichHandler
from rich.table import Table

__version__ = "1.0.0"

# ============================================================================
# UTILS
# ============================================================================


class StreamHasher:
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
    import shutil
    path = shutil.which(binary)
    if path is None:
        raise FileNotFoundError(f"Binaire requis introuvable dans le PATH : '{binary}'. Installe-le avant de continuer.")
    return path


# ============================================================================
# CONFIGURATION (TOML) - meme format que backup.py, lu independamment
# ============================================================================


class ConfigError(Exception):
    """Configuration invalide ou fichier introuvable."""


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


@dataclass
class PathsConfig:
    log_file: str = "logs/vps-backup.log"
    tmp_dir: str = "state"
    prefix: str = "vps-debian/"


@dataclass
class Config:
    s3: S3Config
    backup: BackupConfig
    paths: PathsConfig
    base_dir: Path

    @property
    def log_file(self) -> Path:
        p = Path(self.paths.log_file)
        return p if p.is_absolute() else self.base_dir / p

    @property
    def state_dir(self) -> Path:
        p = Path(self.paths.tmp_dir)
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
    )
    p = raw.get("paths", {})
    paths_cfg = PathsConfig(
        log_file=p.get("log_file", "logs/vps-backup.log"),
        tmp_dir=p.get("tmp_dir", "state"),
        prefix=p.get("prefix", "vps-debian/"),
    )
    if not s3_cfg.bucket or not s3_cfg.endpoint.startswith("https://") or not s3_cfg.region:
        raise ConfigError("Section [s3] invalide : bucket/endpoint/region requis, endpoint doit commencer par https://")

    return Config(s3=s3_cfg, backup=backup_cfg, paths=paths_cfg, base_dir=config_path.resolve().parent)


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
# VERROU (evite une restauration concurrente avec une autre operation)
# ============================================================================


class LockError(Exception):
    """Une autre operation vps-backup/restore est deja en cours."""


def _pid_is_alive(pid: int) -> bool:
    import os
    if pid <= 0:
        return False
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    return True


class RestoreLock:
    def __init__(self, lock_path: Path) -> None:
        self.lock_path = lock_path
        self._acquired = False

    def acquire(self) -> None:
        import os
        import time
        self.lock_path.parent.mkdir(parents=True, exist_ok=True)
        if self.lock_path.exists():
            existing_pid = -1
            try:
                info = json.loads(self.lock_path.read_text())
                existing_pid = int(info.get("pid", -1))
            except (ValueError, json.JSONDecodeError, OSError):
                pass
            if _pid_is_alive(existing_pid):
                raise LockError(f"Une operation est deja en cours (pid={existing_pid}). Verrou : {self.lock_path}")
            logger.warning("Verrou obsolete detecte (pid=%s introuvable) - suppression et poursuite", existing_pid)
            self.lock_path.unlink(missing_ok=True)
        self.lock_path.write_text(json.dumps({"pid": os.getpid(), "started": time.strftime("%Y-%m-%dT%H:%M:%S"), "command": "restore"}))
        self._acquired = True

    def release(self) -> None:
        import os
        if not self._acquired:
            return
        try:
            info = json.loads(self.lock_path.read_text())
            if int(info.get("pid", -1)) == os.getpid():
                self.lock_path.unlink(missing_ok=True)
        except (ValueError, json.JSONDecodeError, OSError):
            pass
        self._acquired = False

    def __enter__(self) -> "RestoreLock":
        self.acquire()
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.release()


# ============================================================================
# CLIENT S3 (lecture seule + listing, aucune ecriture necessaire ici)
# ============================================================================


def build_client(cfg: S3Config):
    return boto3.client(
        "s3", endpoint_url=cfg.endpoint, region_name=cfg.region,
        config=BotoConfig(signature_version="s3v4"),
    )


def get_object_bytes(client, bucket: str, key: str) -> bytes:
    return client.get_object(Bucket=bucket, Key=key)["Body"].read()


def get_object_stream(client, bucket: str, key: str):
    return client.get_object(Bucket=bucket, Key=key)["Body"]


def list_objects(client, bucket: str, prefix: str) -> list[dict[str, Any]]:
    objects: list[dict[str, Any]] = []
    paginator = client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        objects.extend(page.get("Contents", []))
    return objects


# ============================================================================
# MANIFEST (lecture seule ici)
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
# PIPELINE DE RESTAURATION : S3 -> pigz -d -> tar x (streaming, sans archive temporaire)
# ============================================================================


class ArchiveError(Exception):
    """Erreur pendant l'extraction (tar ou pigz)."""


class IntegrityError(Exception):
    """La sauvegarde restauree ne correspond pas au manifest (taille/SHA256)."""


@dataclass
class RestorePipeline:
    pigz_proc: subprocess.Popen
    tar_proc: subprocess.Popen

    def write(self, chunk: bytes) -> None:
        self.pigz_proc.stdin.write(chunk)  # type: ignore[union-attr]

    def close(self) -> tuple[int, int]:
        if self.pigz_proc.stdin:
            self.pigz_proc.stdin.close()
        pigz_rc = self.pigz_proc.wait()
        tar_rc = self.tar_proc.wait()
        return tar_rc, pigz_rc


def build_restore_pipeline(dest_dir: Path) -> RestorePipeline:
    which_or_raise("tar")
    which_or_raise("pigz")
    dest_dir.mkdir(parents=True, exist_ok=True)
    pigz_proc = subprocess.Popen(["pigz", "-dc"], stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    tar_proc = subprocess.Popen(["tar", "-xpf", "-", "-C", str(dest_dir)], stdin=pigz_proc.stdout, stderr=subprocess.PIPE)
    if pigz_proc.stdout:
        pigz_proc.stdout.close()
    return RestorePipeline(pigz_proc=pigz_proc, tar_proc=tar_proc)


def run_restore(cfg: Config, backup_key: str, dest_dir: Path, verify: bool = True) -> None:
    client = build_client(cfg.s3)
    manifest_key = manifest_key_for(backup_key)
    logger.info("Lecture du manifest %s", manifest_key)
    m = download_manifest(client, cfg.s3.bucket, manifest_key)

    logger.info("Restauration de %s vers %s (streaming, aucune archive temporaire)", backup_key, dest_dir)
    pipeline = build_restore_pipeline(dest_dir)
    hasher = StreamHasher() if verify else None

    body = get_object_stream(client, cfg.s3.bucket, backup_key)
    total = 0
    chunk_size = cfg.backup.buffer_mb * 1024 * 1024
    try:
        for chunk in body.iter_chunks(chunk_size=chunk_size):
            pipeline.write(chunk)
            if hasher is not None:
                hasher.update(chunk)
            total += len(chunk)
        tar_rc, pigz_rc = pipeline.close()
        if tar_rc != 0:
            raise ArchiveError(f"tar (extraction) a echoue, code {tar_rc}")
        if pigz_rc != 0:
            raise ArchiveError(f"pigz (decompression) a echoue, code {pigz_rc}")
    except Exception:
        logger.exception("Echec pendant la restauration")
        raise

    logger.info("Flux restaure : %s", human_size(total))

    if verify:
        assert hasher is not None
        if total != m.total_size:
            raise IntegrityError(f"Taille incoherente : attendu {m.total_size}, obtenu {total}")
        if hasher.hexdigest() != m.sha256:
            raise IntegrityError(f"SHA256 incoherent : attendu {m.sha256}, obtenu {hasher.hexdigest()}")
        logger.info("Integrite verifiee (taille + SHA256 corrects)")

    logger.info("Restauration terminee dans %s", dest_dir)


# ============================================================================
# CLI (Typer)
# ============================================================================

app = typer.Typer(add_completion=False, help="Restauration et listing des sauvegardes VPS (voir backup.py pour la sauvegarde).")
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


@app.command("list")
def list_backups(config: Path = ConfigOption) -> None:
    """Liste les sauvegardes disponibles dans le bucket S3."""
    cfg = _load(config)
    client = build_client(cfg.s3)
    manifests = list_all_manifests(client, cfg.s3.bucket, cfg.paths.prefix)

    if not manifests:
        console.print("Aucune sauvegarde trouvee.")
        return

    table = Table(title="Sauvegardes disponibles")
    table.add_column("Cle S3")
    table.add_column("Date")
    table.add_column("Taille")
    table.add_column("SHA256")
    for m in manifests:
        table.add_row(m.backup_key, m.created, human_size(m.total_size), m.sha256[:16] + "...")
    console.print(table)


@app.command()
def restore(
    backup_key: str = typer.Argument(..., help="Cle S3 de la sauvegarde a restaurer (voir 'restore.py list')"),
    dest: Path = typer.Argument(..., help="Repertoire de destination"),
    config: Path = ConfigOption,
    no_verify: bool = typer.Option(False, "--no-verify", help="Desactive la verification SHA256 post-restauration"),
) -> None:
    """Restaure une sauvegarde en streaming (aucune archive temporaire)."""
    cfg = _load(config)
    lock = RestoreLock(cfg.state_dir / "restore.lock")
    try:
        with lock:
            run_restore(cfg, backup_key, dest, verify=not no_verify)
        console.print(f"[green]Restauration terminee dans[/green] {dest}")
    except LockError as exc:
        console.print(f"[red]{exc}[/red]")
        raise typer.Exit(code=1)
    except Exception as exc:
        console.print(f"[red]Echec de la restauration :[/red] {exc}")
        raise typer.Exit(code=1)


@app.command()
def version() -> None:
    """Affiche la version de vps-restore."""
    console.print(f"vps-restore {__version__}")


if __name__ == "__main__":
    app()
