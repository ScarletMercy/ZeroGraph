"""Cache system for node-level result caching."""

from __future__ import annotations

import time
from abc import ABC, abstractmethod
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

__all__ = ("BaseCache", "InMemoryCache", "CachePolicy")


@dataclass(frozen=True)
class CachePolicy:
    """Configuration for node-level caching."""
    key_func: Callable[[str, Any], str] | None = None
    ttl: float | None = None


class BaseCache(ABC):
    """Abstract base class for cache backends."""

    @abstractmethod
    def get(self, key: str) -> Any | None:
        """Retrieve a cached value by key. Returns None if not found or expired."""

    @abstractmethod
    def set(self, key: str, value: Any, ttl: float | None = None) -> None:
        """Store a value with optional TTL override."""

    @abstractmethod
    def clear(self) -> None:
        """Remove all cached entries."""


class InMemoryCache(BaseCache):
    """In-memory cache with TTL support and size limit."""

    def __init__(self, maxsize: int = 256) -> None:
        self._store: dict[str, tuple[Any, float | None]] = {}
        self._maxsize = maxsize

    def _evict(self) -> None:
        if not self._store:
            return
        now = time.monotonic()
        expired = [k for k, (_, exp) in self._store.items() if exp is not None and now > exp]
        if expired:
            for k in expired:
                del self._store[k]
            return
        oldest = next(iter(self._store))
        del self._store[oldest]

    def get(self, key: str) -> Any | None:
        entry = self._store.get(key)
        if entry is None:
            return None
        value, expires_at = entry
        if expires_at is not None and time.monotonic() > expires_at:
            self._store.pop(key, None)
            return None
        return value

    def set(self, key: str, value: Any, ttl: float | None = None) -> None:
        if self._maxsize <= 0:
            return
        if key not in self._store and len(self._store) >= self._maxsize:
            self._evict()
        expires_at = None
        if ttl is not None:
            expires_at = time.monotonic() + ttl
        self._store[key] = (value, expires_at)

    def clear(self) -> None:
        self._store.clear()
