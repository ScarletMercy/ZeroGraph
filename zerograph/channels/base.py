"""ZeroGraph channel base class."""

from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Sequence
from typing import Any, Generic, TypeVar

from zerograph._internal import MISSING
from zerograph.errors import EmptyChannelError

Value = TypeVar("Value")
Update = TypeVar("Update")
Checkpoint = TypeVar("Checkpoint")

__all__ = ("BaseChannel",)


class BaseChannel(Generic[Value, Update, Checkpoint], ABC):
    """Base class for all channels."""

    __slots__ = ("key", "typ")

    def __init__(self, typ: Any, key: str = "") -> None:
        self.typ = typ
        self.key = key

    @property
    @abstractmethod
    def ValueType(self) -> Any: ...

    @property
    @abstractmethod
    def UpdateType(self) -> Any: ...

    def copy(self) -> BaseChannel:
        return self.from_checkpoint(self.checkpoint())

    def checkpoint(self) -> Any:
        try:
            return self.get()
        except EmptyChannelError:
            return MISSING

    @abstractmethod
    def from_checkpoint(self, checkpoint: Any) -> BaseChannel: ...

    @abstractmethod
    def get(self) -> Value: ...

    def is_available(self) -> bool:
        try:
            self.get()
            return True
        except EmptyChannelError:
            return False

    @abstractmethod
    def update(self, values: Sequence[Update]) -> bool: ...

    def consume(self) -> bool:
        return False

    def finish(self) -> bool:
        return False
