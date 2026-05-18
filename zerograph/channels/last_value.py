"""LastValue channel - stores the last value received."""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any, Generic

from zerograph._internal import MISSING
from zerograph.channels.base import BaseChannel, Value
from zerograph.errors import EmptyChannelError, InvalidUpdateError

__all__ = ("LastValue", "LastValueAfterFinish")


class LastValue(Generic[Value], BaseChannel[Value, Value, Value]):
    """Stores the last value received, at most one per step."""

    __slots__ = ("value",)

    def __init__(self, typ: Any, key: str = "") -> None:
        super().__init__(typ, key)
        self.value = MISSING

    def __eq__(self, other: object) -> bool:
        return isinstance(other, LastValue)

    def __hash__(self) -> int:
        return hash(("LastValue",))

    @property
    def ValueType(self) -> type[Value]:
        return self.typ

    @property
    def UpdateType(self) -> type[Value]:
        return self.typ

    def copy(self) -> LastValue:
        empty = self.__class__(self.typ, self.key)
        empty.value = self.value
        return empty

    def from_checkpoint(self, checkpoint: Value) -> LastValue:
        empty = self.__class__(self.typ, self.key)
        if checkpoint is not MISSING:
            empty.value = checkpoint
        return empty

    def update(self, values: Sequence[Value]) -> bool:
        if len(values) == 0:
            return False
        if len(values) != 1:
            raise InvalidUpdateError(
                f"At key '{self.key}': Can receive only one value per step. "
                "Use an Annotated key with a reducer to handle multiple values."
            )
        self.value = values[-1]
        return True

    def get(self) -> Value:
        if self.value is MISSING:
            raise EmptyChannelError()
        return self.value

    def is_available(self) -> bool:
        return self.value is not MISSING

    def checkpoint(self) -> Value:
        return self.value


class LastValueAfterFinish(Generic[Value], BaseChannel[Value, Value, tuple]):
    """Like LastValue but only readable after finish() is called."""

    __slots__ = ("value", "finished")

    def __init__(self, typ: Any, key: str = "") -> None:
        super().__init__(typ, key)
        self.value = MISSING
        self.finished = False

    def __eq__(self, other: object) -> bool:
        return isinstance(other, LastValueAfterFinish)

    def __hash__(self) -> int:
        return hash(("LastValueAfterFinish",))

    @property
    def ValueType(self) -> type[Value]:
        return self.typ

    @property
    def UpdateType(self) -> type[Value]:
        return self.typ

    def checkpoint(self):
        if self.value is MISSING:
            return MISSING
        return (self.value, self.finished)

    def from_checkpoint(self, checkpoint) -> LastValueAfterFinish:
        empty = self.__class__(self.typ)
        empty.key = self.key
        if checkpoint is not MISSING:
            empty.value, empty.finished = checkpoint
        return empty

    def update(self, values: Sequence) -> bool:
        if len(values) == 0:
            return False
        if len(values) != 1:
            raise InvalidUpdateError(
                f"At key '{self.key}': Can receive only one value per step. "
                "Use an Annotated key with a reducer to handle multiple values."
            )
        self.finished = False
        self.value = values[-1]
        return True

    def consume(self) -> bool:
        if self.finished:
            self.finished = False
            self.value = MISSING
            return True
        return False

    def finish(self) -> bool:
        if not self.finished and self.value is not MISSING:
            self.finished = True
            return True
        return False

    def get(self) -> Value:
        if self.value is MISSING or not self.finished:
            raise EmptyChannelError()
        return self.value

    def is_available(self) -> bool:
        return self.value is not MISSING and self.finished
