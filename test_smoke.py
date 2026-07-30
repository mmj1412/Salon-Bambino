"""Test de bout en bout pour la version simplifiee (backup.py + restore.py).
Utilise moto (S3 simule). Non requis en production, sert de non-regression.
"""
from __future__ import annotations

import filecmp
import importlib.util
import logging
import os
import shutil
import sys
import tempfile
from pathlib import Path

from moto import mock_aws

HERE = Path(__file__).resolve().parent

try:
    import tomllib  # noqa: F401
except ModuleNotFoundError:
    import tomli
    sys.modules["tomllib"] = tomli

import boto3

logging.basicConfig(level=logging.INFO, format="%(message)s")
log = logging.getLogger("test")


def load_module(name: str, filename: str):
    spec = importlib.util.spec_from_file_location(name, HERE / filename)
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module  # necessaire pour dataclasses (resolution via sys.modules)
    spec.loader.exec_module(module)
    return module


backup_mod = load_module("vps_backup_backup", "backup.py")
restore_mod = load_module("vps_backup_restore", "restore.py")


def make_source_tree(root: Path) -> None:
    root.mkdir(parents=True, exist_ok=True)
    (root / "sub").mkdir(exist_ok=True)
    for i in range(6):
        (root / f"file_{i}.bin").write_bytes(os.urandom(5 * 1024 * 1024))
    (root / "sub" / "text.txt").write_text("contenu de test\n" * 1000)


def build_backup_cfg(base_dir: Path, prefix: str, part_size_mb: int, threads: int) -> "backup_mod.Config":
    return backup_mod.Config(
        s3=backup_mod.S3Config(bucket="testbucket", endpoint="https://s3.eu-west-par.io.cloud.ovh.net", region="eu-west-1"),
        backup=backup_mod.BackupConfig(retention=2, compression_level=1, part_size_mb=part_size_mb, buffer_mb=1, one_file_system=False, exclude=[]),
        upload=backup_mod.UploadConfig(threads=threads, retries=2, retry_backoff_seconds=0.1),
        paths=backup_mod.PathsConfig(log_file="logs/test.log", tmp_dir="state_test", prefix=prefix),
        base_dir=base_dir,
    )


def build_restore_cfg(base_dir: Path, prefix: str) -> "restore_mod.Config":
    return restore_mod.Config(
        s3=restore_mod.S3Config(bucket="testbucket", endpoint="https://s3.eu-west-par.io.cloud.ovh.net", region="eu-west-1"),
        backup=restore_mod.BackupConfig(buffer_mb=1),
        paths=restore_mod.PathsConfig(log_file="logs/test.log", tmp_dir="state_test", prefix=prefix),
        base_dir=base_dir,
    )


def run() -> None:
    workdir = Path(tempfile.mkdtemp(prefix="vps-backup-simple-test-"))
    source = workdir / "source"
    make_source_tree(source)

    backup_mod.build_client = lambda s3_cfg: boto3.client("s3", region_name=s3_cfg.region)
    restore_mod.build_client = lambda s3_cfg: boto3.client("s3", region_name=s3_cfg.region)

    with mock_aws():
        client = boto3.client("s3", region_name="eu-west-1")
        client.create_bucket(Bucket="testbucket", CreateBucketConfiguration={"LocationConstraint": "eu-west-1"})

        # --- Test A : sauvegarde + restauration ---
        log.info("=== Test A : sauvegarde complete ===")
        cfg = build_backup_cfg(workdir, "test/", part_size_mb=5, threads=2)
        result = backup_mod.run_backup_upload(cfg, sources=[str(source)])
        assert len(result.parts) >= 2

        m = backup_mod.Manifest(
            hostname="h", created="2026-07-30T00:00:00Z", compression="pigz",
            compression_level=1, part_size_mb=5, total_size=result.total_size, sha256=result.sha256,
            backup_key=result.key, upload_id=result.upload_id, parts=result.parts,
        )
        backup_mod.upload_manifest(m, client, cfg.s3.bucket)
        log.info("OK : %d parties, manifest uploade", len(result.parts))

        rcfg = build_restore_cfg(workdir, "test/")
        backups = restore_mod.list_all_manifests(client, rcfg.s3.bucket, rcfg.paths.prefix)
        assert len(backups) == 1
        log.info("OK : restore.py list retrouve bien la sauvegarde")

        dest = workdir / "restore"
        restore_mod.run_restore(rcfg, result.key, dest, verify=True)
        restored_source = dest / source.relative_to(source.anchor)
        for f in ["file_0.bin", "file_1.bin", "sub/text.txt"]:
            assert filecmp.cmp(source / f, restored_source / f, shallow=False), f"contenu different : {f}"
        log.info("OK : restauration + verification SHA256 + contenu identique")

        # --- Test B : reprise apres interruption + garde-fou trous de sequence ---
        log.info("=== Test B : reprise apres interruption ===")
        cfg2 = build_backup_cfg(workdir, "test2/", part_size_mb=5, threads=1)

        real_upload_part = backup_mod.upload_part
        call_count = {"n": 0}

        def flaky_upload_part(client_, bucket, key, upload_id, part_number, body):
            call_count["n"] += 1
            if call_count["n"] == 2:
                raise RuntimeError("panne reseau simulee")
            return real_upload_part(client_, bucket, key, upload_id, part_number, body)

        backup_mod.upload_part = flaky_upload_part
        try:
            try:
                backup_mod.run_backup_upload(cfg2, sources=[str(source)])
                raise AssertionError("la panne simulee aurait du interrompre la sauvegarde")
            except AssertionError:
                raise
            except Exception as exc:
                log.info("OK : interruption simulee comme prevu (%s)", exc)
        finally:
            backup_mod.upload_part = real_upload_part

        resume_file = cfg2.state_dir / "resume.json"
        assert resume_file.exists()
        result2 = backup_mod.run_backup_upload(cfg2, sources=[str(source)])
        assert not resume_file.exists()
        log.info("OK : reprise reussie apres panne, resume.json nettoye, %d parties", len(result2.parts))
        assert result2.sha256 == result.sha256, "SHA256 global doit etre identique (meme source)"

        m2 = backup_mod.Manifest(
            hostname="h", created="2026-07-30T00:00:00Z", compression="pigz", compression_level=1,
            part_size_mb=5, total_size=result2.total_size, sha256=result2.sha256,
            backup_key=result2.key, upload_id=result2.upload_id, parts=result2.parts,
        )
        backup_mod.upload_manifest(m2, client, cfg2.s3.bucket)
        rcfg2 = build_restore_cfg(workdir, "test2/")
        dest2 = workdir / "restore2"
        restore_mod.run_restore(rcfg2, result2.key, dest2, verify=True)
        restored_source2 = dest2 / source.relative_to(source.anchor)
        assert filecmp.cmp(source / "file_0.bin", restored_source2 / "file_0.bin", shallow=False)
        log.info("OK : sauvegarde reprise restauree correctement (integrite verifiee par restore.py)")

        # --- Test C : rotation ---
        log.info("=== Test C : rotation (retention=2) ===")
        cfg3 = build_backup_cfg(workdir, "test/", part_size_mb=5, threads=1)
        for _ in range(3):
            r = backup_mod.run_backup_upload(cfg3, sources=[str(source)])
            mm = backup_mod.Manifest(
                hostname="h", created="2026-07-30T00:00:00Z", compression="pigz", compression_level=1,
                part_size_mb=5, total_size=r.total_size, sha256=r.sha256, backup_key=r.key,
                upload_id=r.upload_id, parts=r.parts,
            )
            backup_mod.upload_manifest(mm, client, cfg3.s3.bucket)
            backup_mod.perform_rotation(cfg3, client)
        remaining = restore_mod.list_all_manifests(client, cfg3.s3.bucket, cfg3.paths.prefix)
        assert len(remaining) == cfg3.backup.retention
        log.info("OK : rotation conserve exactement %d sauvegardes", len(remaining))

    # --- Test D : resolution des sources (chemins fixes + volumes Docker) ---
    log.info("=== Test D : resolution des sources (sans backup reel) ===")
    existing_dir = workdir / "opt_wordpress"
    existing_dir.mkdir()
    (existing_dir / "docker-compose.yml").write_text("services: {}\n")
    missing_dir = str(workdir / "does_not_exist")

    cfg4 = backup_mod.Config(
        s3=backup_mod.S3Config(bucket="testbucket", endpoint="https://s3.eu-west-1.io.cloud.ovh.net", region="eu-west-1"),
        backup=backup_mod.BackupConfig(
            include_paths=[str(existing_dir), missing_dir],
            docker_volumes=["fake_volume_ok", "fake_volume_missing"],
        ),
        upload=backup_mod.UploadConfig(),
        paths=backup_mod.PathsConfig(tmp_dir="state_test4", log_file="logs/test4.log"),
        base_dir=workdir,
    )

    fake_mountpoint = workdir / "docker_volumes" / "fake_volume_ok" / "_data"
    fake_mountpoint.mkdir(parents=True)

    def fake_resolve_volume(name: str) -> str:
        if name == "fake_volume_ok":
            return str(fake_mountpoint)
        raise FileNotFoundError(f"Volume Docker introuvable : '{name}'")

    real_resolve_volume = backup_mod.resolve_docker_volume_mountpoint
    real_generate_pkglist = backup_mod.generate_package_list
    backup_mod.resolve_docker_volume_mountpoint = fake_resolve_volume
    backup_mod.generate_package_list = lambda cfg: None  # pas de dpkg dans ce sandbox de test
    try:
        sources = backup_mod.resolve_backup_sources(cfg4)
    finally:
        backup_mod.resolve_docker_volume_mountpoint = real_resolve_volume
        backup_mod.generate_package_list = real_generate_pkglist

    assert str(existing_dir) in sources, "le chemin existant doit etre inclus"
    assert missing_dir not in sources, "le chemin absent doit etre filtre, pas plante la sauvegarde"
    assert str(fake_mountpoint) in sources, "le volume Docker resolu doit etre inclus"
    assert not any("fake_volume_missing" in s for s in sources), "le volume Docker introuvable doit etre ignore"
    log.info("OK : chemin absent + volume Docker introuvable ignores sans erreur, sources valides conservees : %s", sources)

    shutil.rmtree(workdir, ignore_errors=True)
    log.info("=== TOUS LES TESTS SONT PASSES (version simplifiee 2 scripts) ===")


if __name__ == "__main__":
    run()
