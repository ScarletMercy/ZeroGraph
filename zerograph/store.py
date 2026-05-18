"""Store system for long-term key-value memory."""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

__all__ = ("BaseStore", "InMemoryStore", "StoreItem")


@dataclass
class StoreItem:
    """A single item in the store."""
    key: str
    value: Any
    namespace: str = ""
    created_at: str = ""
    updated_at: str = ""

    def __post_init__(self):
        now = datetime.now(timezone.utc).isoformat()
        if not self.created_at:
            self.created_at = now
        if not self.updated_at:
            self.updated_at = now


class BaseStore(ABC):
    """Abstract base class for store backends."""

    @abstractmethod
    def get(self, namespace: str, key: str) -> StoreItem | None:
        """Retrieve an item by namespace and key."""

    @abstractmethod
    def search(self, namespace: str, *, prefix: str = "",
               limit: int = 10) -> list[StoreItem]:
        """Search for items in a namespace by key prefix."""

    @abstractmethod
    def put(self, namespace: str, key: str, value: Any) -> None:
        """Store or update an item."""

    @abstractmethod
    def delete(self, namespace: str, key: str) -> None:
        """Delete an item."""


class InMemoryStore(BaseStore):
    """In-memory store implementation with namespace support."""

    def __init__(self) -> None:
        self._data: dict[str, dict[str, StoreItem]] = {}

    def get(self, namespace: str, key: str) -> StoreItem | None:
        ns = self._data.get(namespace)
        if ns is None:
            return None
        return ns.get(key)

    def search(self, namespace: str, *, prefix: str = "",
               limit: int = 10) -> list[StoreItem]:
        ns = self._data.get(namespace, {})
        items = [
            item for key, item in ns.items()
            if key.startswith(prefix)
        ]
        items.sort(key=lambda x: x.updated_at, reverse=True)
        return items[:limit]

    def put(self, namespace: str, key: str, value: Any) -> None:
        now = datetime.now(timezone.utc).isoformat()
        if namespace not in self._data:
            self._data[namespace] = {}
        existing = self._data[namespace].get(key)
        if existing is not None:
            existing.value = value
            existing.updated_at = now
        else:
            self._data[namespace][key] = StoreItem(
                key=key, value=value, namespace=namespace,
                created_at=now, updated_at=now,
            )

    def delete(self, namespace: str, key: str) -> None:
        ns = self._data.get(namespace)
        if ns is not None:
            ns.pop(key, None)
