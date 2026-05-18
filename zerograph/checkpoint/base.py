"""ZeroGraph checkpoint base types."""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any, NamedTuple, TypedDict

from zerograph._internal import MISSING

__all__ = (
    "Checkpoint",
    "CheckpointMetadata",
    "CheckpointTuple",
    "BaseCheckpointSaver",
    "ChannelVersions",
    "PendingWrite",
    "V",
)

V = int
ChannelVersions = dict[str, V]
PendingWrite = tuple[str, str, Any]


class CheckpointMetadata(TypedDict, total=False):
    source: str
    step: int
    parents: dict[str, str]
    run_id: str


class Checkpoint(TypedDict, total=False):
    v: int
    id: str
    ts: str
    channel_values: dict[str, Any]
    channel_versions: ChannelVersions
    versions_seen: dict[str, ChannelVersions]
    updated_channels: list[str] | None


class CheckpointTuple(NamedTuple):
    config: dict
    checkpoint: Checkpoint
    metadata: CheckpointMetadata
    parent_config: dict | None = None
    pending_writes: list[PendingWrite] | None = None


class BaseCheckpointSaver(ABC):
    """Abstract base class for checkpoint savers."""

    @abstractmethod
    def get_tuple(self, config: dict) -> CheckpointTuple | None:
        """Get a checkpoint tuple by config."""
        ...

    def get(self, config: dict) -> Checkpoint | None:
        """Get a checkpoint by config."""
        tup = self.get_tuple(config)
        return tup.checkpoint if tup else None

    @abstractmethod
    def put(
        self, config: dict, checkpoint: Checkpoint, metadata: CheckpointMetadata
    ) -> dict:
        """Store a checkpoint. Returns updated config."""
        ...

    @abstractmethod
    def put_writes(
        self, config: dict, writes: list[PendingWrite], task_id: str
    ) -> None:
        """Store pending writes for a task."""
        ...

    @abstractmethod
    def list(self, config: dict, *, limit: int = 10, before: dict | None = None) -> list[CheckpointTuple]:
        """List checkpoints."""
        ...

    def delete_thread(self, thread_id: str) -> None:
        """Delete all checkpoints for a thread."""
        raise NotImplementedError
