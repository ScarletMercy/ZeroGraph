"""SQLite-backed checkpoint saver for persistent state."""

from __future__ import annotations

import asyncio
import json
import sqlite3
import threading
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from zerograph.checkpoint.base import (
    BaseCheckpointSaver,
    Checkpoint,
    CheckpointMetadata,
    CheckpointTuple,
    PendingWrite,
)

__all__ = ("SqliteSaver", "AsyncSqliteSaver")


_ZG_TYPE = "__zerograph_type__"


class _CheckpointEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, frozenset):
            return {_ZG_TYPE: "frozenset", "data": sorted(obj, key=repr)}
        if isinstance(obj, set):
            return {_ZG_TYPE: "set", "data": sorted(obj, key=repr)}
        if isinstance(obj, tuple):
            return {_ZG_TYPE: "tuple", "data": list(obj)}
        if isinstance(obj, bytes):
            return {_ZG_TYPE: "bytes", "data": obj.hex()}
        if isinstance(obj, datetime):
            return {_ZG_TYPE: "datetime", "data": obj.isoformat()}
        if isinstance(obj, timedelta):
            return {_ZG_TYPE: "timedelta", "data": obj.total_seconds()}
        raise TypeError(f"Object of type {type(obj).__name__} is not JSON serializable")


def _decode_obj(obj):
    if isinstance(obj, dict) and _ZG_TYPE in obj:
        t = obj[_ZG_TYPE]
        if t == "set":
            if "data" in obj:
                return set(obj["data"])
        if t == "frozenset":
            if "data" in obj:
                return frozenset(obj["data"])
        if t == "tuple":
            if "data" in obj:
                return tuple(obj["data"])
        if t == "bytes":
            if "data" in obj:
                return bytes.fromhex(obj["data"])
        if t == "datetime":
            if "data" in obj:
                return datetime.fromisoformat(obj["data"])
        if t == "timedelta":
            if "data" in obj:
                return timedelta(seconds=obj["data"])
    return obj


def _dump_json(obj) -> str:
    return json.dumps(obj, cls=_CheckpointEncoder)


def _load_json(text: str) -> Any:
    return json.loads(text, object_hook=_decode_obj)


def _new_checkpoint_id() -> str:
    return str(uuid.uuid4())


def _iso_now() -> str:
    return datetime.now(timezone.utc).isoformat()


_CREATE_TABLES = """
CREATE TABLE IF NOT EXISTS checkpoints (
    thread_id TEXT NOT NULL,
    checkpoint_ns TEXT NOT NULL DEFAULT '',
    checkpoint_id TEXT NOT NULL,
    parent_checkpoint_id TEXT,
    checkpoint TEXT NOT NULL,
    metadata TEXT NOT NULL,
    created_at TEXT NOT NULL,
    PRIMARY KEY (thread_id, checkpoint_ns, checkpoint_id)
);

CREATE TABLE IF NOT EXISTS pending_writes (
    thread_id TEXT NOT NULL,
    checkpoint_ns TEXT NOT NULL DEFAULT '',
    checkpoint_id TEXT NOT NULL,
    task_id TEXT NOT NULL,
    channel TEXT NOT NULL,
    value TEXT NOT NULL,
    PRIMARY KEY (thread_id, checkpoint_ns, checkpoint_id, task_id, channel)
);

CREATE INDEX IF NOT EXISTS idx_checkpoints_ts
    ON checkpoints(thread_id, checkpoint_ns, created_at);
"""


class SqliteSaver(BaseCheckpointSaver):
    """SQLite-backed checkpoint storage with WAL mode and thread-safe connections."""

    def __init__(self, conn_string: str | Path = "checkpoints.db") -> None:
        self._conn_string = str(conn_string)
        self._local = threading.local()
        self._lock = threading.Lock()
        self._connections: set[sqlite3.Connection] = set()
        self._get_conn()

    def _get_conn(self) -> sqlite3.Connection:
        if not hasattr(self._local, "conn") or self._local.conn is None:
            conn = sqlite3.connect(self._conn_string, check_same_thread=False)
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("PRAGMA foreign_keys=ON")
            conn.execute("PRAGMA busy_timeout=5000")
            conn.row_factory = sqlite3.Row
            try:
                self._setup_conn(conn)
            except Exception:
                conn.close()
                self._local.conn = None
                raise
            with self._lock:
                self._connections.add(conn)
            self._local.conn = conn
        return self._local.conn

    def _setup_conn(self, conn: sqlite3.Connection) -> None:
        conn.executescript(_CREATE_TABLES)
        conn.commit()

    def close(self) -> None:
        with self._lock:
            conns = list(self._connections)
            self._connections.clear()
        for conn in conns:
            try:
                conn.close()
            except Exception:
                pass
        self._local = threading.local()

    def __enter__(self) -> SqliteSaver:
        return self

    def __exit__(self, *args: Any) -> None:
        self.close()

    def _get_thread_id(self, config: dict) -> str:
        return config.get("configurable", {}).get("thread_id", "__default__")

    def _get_checkpoint_ns(self, config: dict) -> str:
        return config.get("configurable", {}).get("checkpoint_ns", "")

    def _get_checkpoint_id(self, config: dict) -> str | None:
        return config.get("configurable", {}).get("checkpoint_id")

    def get_tuple(self, config: dict) -> CheckpointTuple | None:
        conn = self._get_conn()
        thread_id = self._get_thread_id(config)
        checkpoint_ns = self._get_checkpoint_ns(config)
        checkpoint_id = self._get_checkpoint_id(config)

        if checkpoint_id:
            row = conn.execute(
                "SELECT checkpoint_id, parent_checkpoint_id, checkpoint, metadata, created_at "
                "FROM checkpoints WHERE thread_id=? AND checkpoint_ns=? AND checkpoint_id=?",
                (thread_id, checkpoint_ns, checkpoint_id),
            ).fetchone()
            if row is None:
                return None
        else:
            row = conn.execute(
                "SELECT checkpoint_id, parent_checkpoint_id, checkpoint, metadata, created_at "
                "FROM checkpoints WHERE thread_id=? AND checkpoint_ns=? "
                "ORDER BY created_at DESC LIMIT 1",
                (thread_id, checkpoint_ns),
            ).fetchone()
            if row is None:
                return None

        checkpoint = _load_json(row["checkpoint"])
        metadata = _load_json(row["metadata"])
        parent_checkpoint_id = row["parent_checkpoint_id"]
        fetched_id = row["checkpoint_id"]

        pending_writes = self._load_pending_writes(
            conn, thread_id, checkpoint_ns, fetched_id
        )

        parent_config = None
        if parent_checkpoint_id:
            parent_config = {
                "configurable": {
                    "thread_id": thread_id,
                    "checkpoint_ns": checkpoint_ns,
                    "checkpoint_id": parent_checkpoint_id,
                }
            }

        return CheckpointTuple(
            config={
                "configurable": {
                    "thread_id": thread_id,
                    "checkpoint_ns": checkpoint_ns,
                    "checkpoint_id": fetched_id,
                }
            },
            checkpoint=checkpoint,
            metadata=metadata,
            parent_config=parent_config,
            pending_writes=pending_writes,
        )

    def put(
        self, config: dict, checkpoint: Checkpoint, metadata: CheckpointMetadata
    ) -> dict:
        conn = self._get_conn()
        thread_id = self._get_thread_id(config)
        checkpoint_ns = self._get_checkpoint_ns(config)
        checkpoint_id = checkpoint.get("id")
        if not checkpoint_id:
            checkpoint_id = _new_checkpoint_id()
            checkpoint = dict(checkpoint)
            checkpoint["id"] = checkpoint_id

        parent_config_id = config.get("configurable", {}).get("checkpoint_id")

        conn.execute(
            "INSERT OR REPLACE INTO checkpoints "
            "(thread_id, checkpoint_ns, checkpoint_id, parent_checkpoint_id, "
            "checkpoint, metadata, created_at) VALUES (?, ?, ?, ?, ?, ?, ?)",
            (
                thread_id,
                checkpoint_ns,
                checkpoint_id,
                parent_config_id,
                _dump_json(checkpoint),
                _dump_json(metadata),
                checkpoint.get("ts") or _iso_now(),
            ),
        )
        conn.commit()

        return {
            "configurable": {
                "thread_id": thread_id,
                "checkpoint_ns": checkpoint_ns,
                "checkpoint_id": checkpoint_id,
            }
        }

    def put_writes(
        self, config: dict, writes: list[PendingWrite], task_id: str
    ) -> None:
        from zerograph.types import Interrupt as InterruptCls

        conn = self._get_conn()
        thread_id = self._get_thread_id(config)
        checkpoint_ns = self._get_checkpoint_ns(config)
        checkpoint_id = self._get_checkpoint_id(config)
        if not checkpoint_id:
            raise ValueError("checkpoint_id is required in config to store writes")

        for tid, ch, val in writes:
            tid = tid or task_id
            if isinstance(val, InterruptCls):
                val = {"__interrupt__": True, "value": val.value, "id": val.id}
            conn.execute(
                "INSERT OR REPLACE INTO pending_writes "
                "(thread_id, checkpoint_ns, checkpoint_id, task_id, channel, value) "
                "VALUES (?, ?, ?, ?, ?, ?)",
                (
                    thread_id,
                    checkpoint_ns,
                    checkpoint_id,
                    tid,
                    ch,
                    _dump_json(val),
                ),
            )
        conn.commit()

    def list(
        self, config: dict, *, limit: int = 10, before: dict | None = None
    ) -> list[CheckpointTuple]:
        conn = self._get_conn()
        thread_id = self._get_thread_id(config)
        checkpoint_ns = self._get_checkpoint_ns(config)

        if before:
            before_id = before.get("configurable", {}).get("checkpoint_id")
            if before_id:
                before_ts = conn.execute(
                    "SELECT created_at FROM checkpoints "
                    "WHERE thread_id=? AND checkpoint_ns=? AND checkpoint_id=?",
                    (thread_id, checkpoint_ns, before_id),
                ).fetchone()
                if before_ts:
                    rows = conn.execute(
                        "SELECT checkpoint_id, parent_checkpoint_id, checkpoint, metadata, created_at "
                        "FROM checkpoints WHERE thread_id=? AND checkpoint_ns=? AND created_at<? "
                        "ORDER BY created_at DESC LIMIT ?",
                        (thread_id, checkpoint_ns, before_ts["created_at"], limit),
                    ).fetchall()
                else:
                    rows = []
            else:
                rows = conn.execute(
                    "SELECT checkpoint_id, parent_checkpoint_id, checkpoint, metadata, created_at "
                    "FROM checkpoints WHERE thread_id=? AND checkpoint_ns=? "
                    "ORDER BY created_at DESC LIMIT ?",
                    (thread_id, checkpoint_ns, limit),
                ).fetchall()
        else:
            rows = conn.execute(
                "SELECT checkpoint_id, parent_checkpoint_id, checkpoint, metadata, created_at "
                "FROM checkpoints WHERE thread_id=? AND checkpoint_ns=? "
                "ORDER BY created_at DESC LIMIT ?",
                (thread_id, checkpoint_ns, limit),
            ).fetchall()

        results = []
        for row in rows:
            checkpoint = _load_json(row["checkpoint"])
            metadata = _load_json(row["metadata"])
            fetched_id = row["checkpoint_id"]
            parent_checkpoint_id = row["parent_checkpoint_id"]

            pending_writes = self._load_pending_writes(
                conn, thread_id, checkpoint_ns, fetched_id
            )

            parent_config = None
            if parent_checkpoint_id:
                parent_config = {
                    "configurable": {
                        "thread_id": thread_id,
                        "checkpoint_ns": checkpoint_ns,
                        "checkpoint_id": parent_checkpoint_id,
                    }
                }

            results.append(
                CheckpointTuple(
                    config={
                        "configurable": {
                            "thread_id": thread_id,
                            "checkpoint_ns": checkpoint_ns,
                            "checkpoint_id": fetched_id,
                        }
                    },
                    checkpoint=checkpoint,
                    metadata=metadata,
                    parent_config=parent_config,
                    pending_writes=pending_writes,
                )
            )

        return results

    def delete_thread(self, thread_id: str) -> None:
        conn = self._get_conn()
        conn.execute(
            "DELETE FROM pending_writes WHERE thread_id=?", (thread_id,)
        )
        conn.execute(
            "DELETE FROM checkpoints WHERE thread_id=?", (thread_id,)
        )
        conn.commit()

    def get_pending_writes(self, config: dict) -> list[PendingWrite]:
        conn = self._get_conn()
        thread_id = self._get_thread_id(config)
        checkpoint_ns = self._get_checkpoint_ns(config)
        checkpoint_id = self._get_checkpoint_id(config)
        if not checkpoint_id:
            return []
        return self._load_pending_writes(conn, thread_id, checkpoint_ns, checkpoint_id)

    def _load_pending_writes(
        self,
        conn: sqlite3.Connection,
        thread_id: str,
        checkpoint_ns: str,
        checkpoint_id: str,
    ) -> list[PendingWrite]:
        from zerograph.constants import INTERRUPT
        from zerograph.types import Interrupt as InterruptCls

        rows = conn.execute(
            "SELECT task_id, channel, value FROM pending_writes "
            "WHERE thread_id=? AND checkpoint_ns=? AND checkpoint_id=?",
            (thread_id, checkpoint_ns, checkpoint_id),
        ).fetchall()
        result: list[PendingWrite] = []
        for r in rows:
            ch = r["channel"]
            val = _load_json(r["value"])
            if ch == INTERRUPT and isinstance(val, dict) and val.get("__interrupt__"):
                val = InterruptCls(value=val.get("value"), id=val.get("id"))
            result.append((r["task_id"], ch, val))
        return result


class AsyncSqliteSaver(SqliteSaver):
    """Async wrapper around SqliteSaver using asyncio.to_thread.

    All methods have async variants (``aget_tuple``, ``aput``, etc.) that
    delegate to the synchronous ``SqliteSaver`` methods via
    ``asyncio.to_thread``.  The synchronous methods are also still available.

    For ``:memory:`` databases, all async operations are routed through a
    single-threaded executor so they share the same in-memory database.
    """

    def __init__(self, conn_string: str | Path = "checkpoints.db") -> None:
        # Intentionally duplicates SqliteSaver.__init__ fields instead of
        # calling super().__init__() so that :memory: connections can be
        # routed through a single-thread executor.
        self._conn_string = str(conn_string)
        self._local = threading.local()
        self._lock = threading.Lock()
        self._is_memory = self._conn_string == ":memory:"
        self._connections: set[sqlite3.Connection] = set()
        if self._is_memory:
            import concurrent.futures
            self._executor = concurrent.futures.ThreadPoolExecutor(max_workers=1)
            self._executor.submit(self._get_conn).result()
        else:
            self._executor = None
            self._get_conn()

    async def __aenter__(self) -> AsyncSqliteSaver:
        return self

    async def __aexit__(self, *args: Any) -> None:
        self.close()

    def close(self) -> None:
        if self._executor is not None:
            self._executor.shutdown(wait=False)
            self._executor = None
        super().close()

    async def _to_thread(self, fn, *args, **kwargs):
        if self._is_memory and self._executor is not None:
            import functools
            loop = asyncio.get_running_loop()
            partial_fn = functools.partial(fn, *args, **kwargs)
            return await loop.run_in_executor(self._executor, partial_fn)
        return await asyncio.to_thread(fn, *args, **kwargs)

    async def aget_tuple(self, config: dict) -> CheckpointTuple | None:
        return await self._to_thread(super().get_tuple, config)

    def get_tuple(self, config: dict) -> CheckpointTuple | None:
        return super().get_tuple(config)

    async def aput(
        self, config: dict, checkpoint: Checkpoint, metadata: CheckpointMetadata
    ) -> dict:
        return await self._to_thread(super().put, config, checkpoint, metadata)

    def put(
        self, config: dict, checkpoint: Checkpoint, metadata: CheckpointMetadata
    ) -> dict:
        return super().put(config, checkpoint, metadata)

    async def aput_writes(
        self, config: dict, writes: list[PendingWrite], task_id: str
    ) -> None:
        return await self._to_thread(super().put_writes, config, writes, task_id)

    def put_writes(
        self, config: dict, writes: list[PendingWrite], task_id: str
    ) -> None:
        return super().put_writes(config, writes, task_id)

    async def alist(
        self, config: dict, *, limit: int = 10, before: dict | None = None
    ) -> list[CheckpointTuple]:
        return await self._to_thread(super().list, config, limit=limit, before=before)

    def list(
        self, config: dict, *, limit: int = 10, before: dict | None = None
    ) -> list[CheckpointTuple]:
        return super().list(config, limit=limit, before=before)

    async def adelete_thread(self, thread_id: str) -> None:
        return await self._to_thread(super().delete_thread, thread_id)

    def delete_thread(self, thread_id: str) -> None:
        return super().delete_thread(thread_id)

    async def aget_pending_writes(self, config: dict) -> list[PendingWrite]:
        return await self._to_thread(super().get_pending_writes, config)

    def get_pending_writes(self, config: dict) -> list[PendingWrite]:
        return super().get_pending_writes(config)
