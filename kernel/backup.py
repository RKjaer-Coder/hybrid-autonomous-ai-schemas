from __future__ import annotations

import json
import shutil
import sqlite3
from pathlib import Path
from typing import Any

from migrate import _schema_fidelity_errors, _schema_hash

from .records import canonical_json, sha256_text
from .store import KERNEL_EVENT_SCHEMA_VERSION, KERNEL_POLICY_VERSION, KernelStore

BACKUP_MANIFEST_VERSION = 1
KERNEL_SCHEMA_NAME = "kernel.sql"
GOVERNED_TABLES = {
    "artifact_refs",
    "artifact_governance_records",
    "side_effect_intents",
    "side_effect_receipts",
}


def create_kernel_backup(source_db: str | Path, backup_dir: str | Path) -> dict[str, Any]:
    """Snapshot kernel.db and write a deterministic manifest next to it."""
    source_db = Path(source_db)
    backup_dir = Path(backup_dir)
    backup_dir.mkdir(parents=True, exist_ok=True)
    backup_db = backup_dir / "kernel.db"
    manifest_path = backup_dir / "kernel.backup.manifest.json"

    if backup_db.exists():
        backup_db.unlink()
    with sqlite3.connect(source_db) as source, sqlite3.connect(backup_db) as dest:
        source.execute("PRAGMA wal_checkpoint(FULL);")
        dest.execute("PRAGMA journal_mode=WAL;")
        source.backup(dest)
        dest.commit()

    manifest = build_kernel_backup_manifest(backup_db)
    manifest_path.write_text(canonical_json(manifest) + "\n", encoding="utf-8")
    return manifest


def verify_kernel_backup(backup_db: str | Path, manifest: str | Path | dict[str, Any]) -> dict[str, Any]:
    """Fail closed unless backup contents replay and match the supplied manifest."""
    backup_db = Path(backup_db)
    expected = _load_manifest(manifest)
    actual = build_kernel_backup_manifest(backup_db)
    if actual != expected:
        raise ValueError("kernel backup manifest mismatch")
    return actual


def restore_kernel_backup(
    backup_db: str | Path,
    manifest: str | Path | dict[str, Any],
    restore_db: str | Path,
    *,
    overwrite: bool = False,
) -> dict[str, Any]:
    """Restore kernel.db only after backup and restored copy both verify."""
    backup_db = Path(backup_db)
    restore_db = Path(restore_db)
    expected = verify_kernel_backup(backup_db, manifest)
    restore_db.parent.mkdir(parents=True, exist_ok=True)
    if restore_db.exists() and not overwrite:
        raise FileExistsError(f"restore target already exists: {restore_db}")

    tmp_restore = restore_db.with_name(f".{restore_db.name}.restore-tmp")
    if tmp_restore.exists():
        tmp_restore.unlink()
    shutil.copy2(backup_db, tmp_restore)
    try:
        verify_kernel_backup(tmp_restore, expected)
        if restore_db.exists():
            restore_db.unlink()
        tmp_restore.replace(restore_db)
    except Exception:
        if tmp_restore.exists():
            tmp_restore.unlink()
        raise
    return expected


def build_kernel_backup_manifest(db_path: str | Path) -> dict[str, Any]:
    db_path = Path(db_path)
    schema_path = Path(__file__).resolve().parents[1] / "schemas" / KERNEL_SCHEMA_NAME
    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys=ON;")
        _assert_kernel_schema(conn, schema_path)
        event_summary = _event_summary(conn)
        table_summaries = _table_summaries(conn)
        governed_summary = _governed_summary(conn)
        # Replay after schema/table reads so hash-chain and event schema version drift fail closed.
        KernelStore._replay_from_connection(conn)
    manifest_without_hash = {
        "manifest_version": BACKUP_MANIFEST_VERSION,
        "database": "kernel.db",
        "schema": {
            "name": KERNEL_SCHEMA_NAME,
            "hash": _schema_hash(schema_path),
        },
        "kernel": {
            "event_schema_version": KERNEL_EVENT_SCHEMA_VERSION,
            "policy_version": KERNEL_POLICY_VERSION,
        },
        "events": event_summary,
        "tables": table_summaries,
        "governed_records": governed_summary,
    }
    return {
        **manifest_without_hash,
        "manifest_hash": sha256_text(canonical_json(manifest_without_hash)),
    }


def _assert_kernel_schema(conn: sqlite3.Connection, schema_path: Path) -> None:
    errors = _schema_fidelity_errors(conn, "kernel", schema_path)
    if errors:
        raise ValueError("kernel schema drift: " + "; ".join(errors))
    meta = _schema_meta(conn)
    if meta is not None and meta.get(KERNEL_SCHEMA_NAME) != _schema_hash(schema_path):
        raise ValueError("kernel schema meta hash mismatch")


def _event_summary(conn: sqlite3.Connection) -> dict[str, Any]:
    row = conn.execute(
        """
        SELECT COUNT(*) AS count, MAX(event_seq) AS last_event_seq
        FROM events
        """
    ).fetchone()
    last = conn.execute(
        """
        SELECT event_id, event_hash, event_schema_version
        FROM events
        ORDER BY event_seq DESC
        LIMIT 1
        """
    ).fetchone()
    versions = [
        int(r[0])
        for r in conn.execute("SELECT DISTINCT event_schema_version FROM events ORDER BY event_schema_version").fetchall()
    ]
    if any(version != KERNEL_EVENT_SCHEMA_VERSION for version in versions):
        raise ValueError("unsupported event schema version in backup")
    return {
        "count": int(row["count"]),
        "last_event_seq": int(row["last_event_seq"] or 0),
        "last_event_id": None if last is None else last["event_id"],
        "last_event_hash": None if last is None else last["event_hash"],
        "event_schema_versions": versions,
    }


def _table_summaries(conn: sqlite3.Connection) -> dict[str, dict[str, Any]]:
    return {table: _table_summary(conn, table) for table in _kernel_tables(conn)}


def _table_summary(conn: sqlite3.Connection, table: str) -> dict[str, Any]:
    rows = [_row_dict(row) for row in conn.execute(f'SELECT * FROM "{table}"').fetchall()]
    rows.sort(key=canonical_json)
    return {
        "row_count": len(rows),
        "content_hash": sha256_text(canonical_json(rows)),
    }


def _governed_summary(conn: sqlite3.Connection) -> dict[str, Any]:
    summaries = {table: _table_summary(conn, table) for table in sorted(GOVERNED_TABLES)}
    missing = _missing_governed_records(conn)
    if missing:
        raise ValueError("missing governed records: " + "; ".join(missing))
    return {
        "tables": summaries,
        "receipt_required_actions": ["redact", "delete", "crypto_shred"],
        "missing_governed_records": [],
    }


def _missing_governed_records(conn: sqlite3.Connection) -> list[str]:
    missing: list[str] = []
    orphan_artifact_governance = conn.execute(
        """
        SELECT record_id FROM artifact_governance_records
        WHERE artifact_id NOT IN (SELECT artifact_id FROM artifact_refs)
        ORDER BY record_id
        """
    ).fetchall()
    missing.extend(f"artifact_governance_records/{row[0]} missing artifact_ref" for row in orphan_artifact_governance)

    missing_receipts = conn.execute(
        """
        SELECT record_id FROM artifact_governance_records
        WHERE action IN ('redact','delete','crypto_shred')
          AND status = 'applied'
          AND (receipt_ref IS NULL OR receipt_hash IS NULL)
        ORDER BY record_id
        """
    ).fetchall()
    missing.extend(f"artifact_governance_records/{row[0]} missing receipt" for row in missing_receipts)

    orphan_side_effect_receipts = conn.execute(
        """
        SELECT receipt_id FROM side_effect_receipts
        WHERE intent_id NOT IN (SELECT intent_id FROM side_effect_intents)
        ORDER BY receipt_id
        """
    ).fetchall()
    missing.extend(f"side_effect_receipts/{row[0]} missing side_effect_intent" for row in orphan_side_effect_receipts)
    return missing


def _kernel_tables(conn: sqlite3.Connection) -> list[str]:
    return [
        row[0]
        for row in conn.execute(
            """
            SELECT name FROM sqlite_master
            WHERE type='table'
              AND name NOT LIKE 'sqlite_%'
              AND name != '_schema_meta'
            ORDER BY name
            """
        ).fetchall()
    ]


def _schema_meta(conn: sqlite3.Connection) -> dict[str, str] | None:
    try:
        rows = conn.execute("SELECT schema_name, schema_hash FROM _schema_meta ORDER BY schema_name").fetchall()
    except sqlite3.OperationalError as exc:
        if "no such table" not in str(exc).lower():
            raise
        return None
    return {row[0]: row[1] for row in rows}


def _row_dict(row: sqlite3.Row) -> dict[str, Any]:
    return {key: row[key] for key in row.keys()}


def _load_manifest(manifest: str | Path | dict[str, Any]) -> dict[str, Any]:
    if isinstance(manifest, dict):
        return json.loads(canonical_json(manifest))
    return json.loads(Path(manifest).read_text(encoding="utf-8"))

