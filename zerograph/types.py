"""ZeroGraph types."""

from __future__ import annotations

import copy
from collections import deque
from collections.abc import Callable, Hashable, Sequence
from dataclasses import asdict, dataclass, field
from datetime import timedelta
from typing import (
    Any,
    ClassVar,
    Generic,
    Literal,
    NamedTuple,
    TypeVar,
    final,
)
from hashlib import sha256

from zerograph._internal import MISSING
from zerograph.constants import (
    CONFIG_KEY_CHECKPOINT_NS,
    CONFIG_KEY_SCRATCHPAD,
    CONFIG_KEY_SEND,
    RESUME,
)
from zerograph.errors import GraphInterrupt

__all__ = (
    "Send",
    "Command",
    "Interrupt",
    "interrupt",
    "StateSnapshot",
    "PregelTask",
    "RetryPolicy",
    "TimeoutPolicy",
    "StreamWriter",
    "Overwrite",
    "All",
)

All = Literal["*"]

StreamWriter = Callable[[Any], None]


class Interrupt:
    """Information about an interrupt that occurred in a node."""

    __slots__ = ("value", "id")

    def __init__(self, value: Any, id: str | None = None) -> None:
        self.value = value
        if id is None:
            h = sha256(str(value).encode())
            self.id = h.hexdigest()[:32]
        else:
            self.id = id

    def __repr__(self) -> str:
        return f"Interrupt(value={self.value!r}, id={self.id!r})"

    def __eq__(self, other: object) -> bool:
        return (
            isinstance(other, Interrupt)
            and self.value == other.value
            and self.id == other.id
        )

    def __hash__(self) -> int:
        return hash((self.value, self.id))

    @classmethod
    def from_ns(cls, value: Any, ns: str) -> Interrupt:
        h = sha256(ns.encode())
        return cls(value=value, id=h.hexdigest()[:32])


class Send:
    """A message to send to a specific node in the graph."""

    __slots__ = ("node", "arg", "timeout")

    def __init__(
        self,
        /,
        node: str,
        arg: Any,
        *,
        timeout: float | timedelta | None = None,
    ) -> None:
        self.node = node
        self.arg = arg
        if isinstance(timeout, timedelta):
            timeout = timeout.total_seconds()
        self.timeout = timeout

    def __hash__(self) -> int:
        return hash((self.node,))

    def __repr__(self) -> str:
        return f"Send(node={self.node!r}, arg={self.arg!r})"

    def __eq__(self, other: object) -> bool:
        return (
            isinstance(other, Send)
            and self.node == other.node
            and self.arg == other.arg
        )


@dataclass
class Command:
    """Command to update state and control graph flow."""

    graph: str | None = None
    update: Any | None = None
    resume: dict[str, Any] | Any | None = None
    goto: Send | Sequence[Send | str] | str = ()

    PARENT: ClassVar[Literal["__parent__"]] = "__parent__"

    def __repr__(self) -> str:
        parts = []
        if self.update is not None:
            parts.append(f"update={self.update!r}")
        if self.resume is not None:
            parts.append(f"resume={self.resume!r}")
        if self.goto:
            parts.append(f"goto={self.goto!r}")
        if self.graph is not None:
            parts.append(f"graph={self.graph!r}")
        return f"Command({', '.join(parts)})"

    def _update_as_tuples(self) -> Sequence[tuple[str, Any]]:
        if isinstance(self.update, dict):
            return list(self.update.items())
        elif isinstance(self.update, (list, tuple)) and all(
            isinstance(t, tuple) and len(t) == 2 and isinstance(t[0], str)
            for t in self.update
        ):
            return self.update
        elif self.update is not None:
            return [("__root__", self.update)]
        else:
            return []


class RetryPolicy(NamedTuple):
    initial_interval: float = 0.5
    backoff_factor: float = 2.0
    max_interval: float = 128.0
    max_attempts: int = 3
    jitter: bool = True
    retry_on: type[Exception] | Sequence[type[Exception]] | Callable[[Exception], bool] = Exception


@dataclass(frozen=True)
class TimeoutPolicy:
    run_timeout: float | None = None
    idle_timeout: float | None = None


@dataclass
class Overwrite:
    """Bypass a reducer and write directly to a BinaryOperatorAggregate channel."""
    value: Any


class PregelTask(NamedTuple):
    id: str
    name: str
    path: tuple
    error: Exception | None = None
    interrupts: tuple = ()
    state: Any = None
    result: Any | None = None


class StateSnapshot(NamedTuple):
    values: dict[str, Any] | Any
    next: tuple[str, ...]
    config: dict
    metadata: dict | None
    created_at: str | None
    parent_config: dict | None
    tasks: tuple
    interrupts: tuple = ()
    subgraphs: dict | None = None


def interrupt(value: Any) -> Any:
    """Interrupt graph execution from within a node."""
    conf = _get_config()
    if "configurable" not in conf:
        raise RuntimeError(
            "interrupt() must be called from within a graph node. "
            "It cannot be called at module level or outside of node execution."
        )
    configurable = conf["configurable"]
    scratchpad = configurable.get(CONFIG_KEY_SCRATCHPAD)
    if scratchpad is not None:
        idx = scratchpad.interrupt_counter()
        if scratchpad.resume:
            if idx < len(scratchpad.resume):
                configurable[CONFIG_KEY_SEND]([(RESUME, scratchpad.resume)])
                return scratchpad.resume[idx]
        v = scratchpad.get_null_resume(True)
        if v is not None:
            scratchpad.resume.append(v)
            configurable[CONFIG_KEY_SEND]([(RESUME, scratchpad.resume)])
            return v
    raise GraphInterrupt(
        (Interrupt.from_ns(value=value, ns=configurable.get(CONFIG_KEY_CHECKPOINT_NS, "")),)
    )


def _get_config() -> dict:
    """Get the current execution config. Must be called within a node."""
    import zerograph.pregel._loop as _loop_mod
    return _loop_mod._current_config.get({})
