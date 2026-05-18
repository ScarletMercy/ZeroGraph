"""In-memory checkpoint saver."""

from __future__ import annotations

import copy
import uuid
from collections import defaultdict
from datetime import datetime, timezone
from typing import Any

from zerograph._internal import MISSING
from zerograph.checkpoint.base import (
    BaseCheckpointSaver,
    Checkpoint,
    CheckpointMetadata,
    CheckpointTuple,
    PendingWrite,
)

__all__ = ("InMemorySaver",)


def _new_checkpoint_id() -> str:
    return str(uuid.uuid4())


def _iso_now() -> str:
    return datetime.now(timezone.utc).isoformat()


class InMemorySaver(BaseCheckpointSaver):
    """In-memory checkpoint storage."""

    def __init__(self) -> None:
        self.storage: dict[str, dict[str, dict[str, CheckpointTuple]]] = defaultdict(
            lambda: defaultdict(dict)
        )
        self.writes: dict[tuple[str, str, str], list[PendingWrite]] = defaultdict(list)

    def _get_thread_id(self, config: dict) -> str:
        return config.get("configurable", {}).get("thread_id", "__default__")

    def _get_checkpoint_ns(self, config: dict) -> str:
        return config.get("configurable", {}).get("checkpoint_ns", "")

    def _get_checkpoint_id(self, config: dict) -> str | None:
        return config.get("configurable", {}).get("checkpoint_id")

    def get_tuple(self, config: dict) -> CheckpointTuple | None:
        thread_id = self._get_thread_id(config)
        checkpoint_ns = self._get_checkpoint_ns(config)
        checkpoint_id = self._get_checkpoint_id(config)

        thread_storage = self.storage.get(thread_id, {})
        ns_storage = thread_storage.get(checkpoint_ns, {})

        if checkpoint_id:
            if checkpoint_id in ns_storage:
                return ns_storage[checkpoint_id]
            return None

        if not ns_storage:
            return None

        latest_id = max(
            ns_storage.keys(),
            key=lambda x: ns_storage[x].checkpoint.get("ts", ""),
        )
        return ns_storage[latest_id]

    def put(
        self, config: dict, checkpoint: Checkpoint, metadata: CheckpointMetadata
    ) -> dict:
        thread_id = self._get_thread_id(config)
        checkpoint_ns = self._get_checkpoint_ns(config)
        checkpoint_id = checkpoint.get("id", _new_checkpoint_id())

        parent_config = config.get("configurable", {}).get("checkpoint_id")

        tup = CheckpointTuple(
            config={
                "configurable": {
                    "thread_id": thread_id,
                    "checkpoint_ns": checkpoint_ns,
                    "checkpoint_id": checkpoint_id,
                }
            },
            checkpoint=copy.deepcopy(checkpoint),
            metadata=copy.deepcopy(metadata),
            parent_config={"configurable": {"checkpoint_id": parent_config}} if parent_config else None,
            pending_writes=[],
        )

        self.storage[thread_id][checkpoint_ns][checkpoint_id] = tup

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
        thread_id = self._get_thread_id(config)
        checkpoint_ns = self._get_checkpoint_ns(config)
        checkpoint_id = self._get_checkpoint_id(config)
        if not checkpoint_id:
            return

        key = (thread_id, checkpoint_ns, checkpoint_id)
        existing = self.writes.get(key, [])

        new_writes = [(w[0], w[1], w[2]) for w in writes]
        for idx, (tid, ch, val) in enumerate(new_writes):
            # Replace existing write for same task + channel
            found = False
            for i, (etid, ech, eval_) in enumerate(existing):
                if etid == tid and ech == ch:
                    existing[i] = (tid, ch, val)
                    found = True
                    break
            if not found:
                existing.append((tid, ch, val))

        self.writes[key] = existing

    def list(
        self, config: dict, *, limit: int = 10, before: dict | None = None
    ) -> list[CheckpointTuple]:
        thread_id = self._get_thread_id(config)
        checkpoint_ns = self._get_checkpoint_ns(config)

        thread_storage = self.storage.get(thread_id, {})
        ns_storage = thread_storage.get(checkpoint_ns, {})

        tuples = list(ns_storage.values())

        if before:
            before_id = before.get("configurable", {}).get("checkpoint_id")
            if before_id and before_id in ns_storage:
                before_ts = ns_storage[before_id].checkpoint.get("ts", "")
                tuples = [t for t in tuples if t.checkpoint.get("ts", "") < before_ts]

        tuples.sort(key=lambda t: t.checkpoint.get("ts", ""), reverse=True)
        return tuples[:limit]

    def delete_thread(self, thread_id: str) -> None:
        if thread_id in self.storage:
            del self.storage[thread_id]
        keys_to_remove = [k for k in self.writes if k[0] == thread_id]
        for k in keys_to_remove:
            del self.writes[k]

    def get_pending_writes(self, config: dict) -> list[PendingWrite]:
        thread_id = self._get_thread_id(config)
        checkpoint_ns = self._get_checkpoint_ns(config)
        checkpoint_id = self._get_checkpoint_id(config)
        if not checkpoint_id:
            return []
        return self.writes.get((thread_id, checkpoint_ns, checkpoint_id), [])
