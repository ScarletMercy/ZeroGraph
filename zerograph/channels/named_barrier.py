"""NamedBarrierValue channel - waits for all named values before available."""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any, Generic

from zerograph._internal import MISSING
from zerograph.channels.base import BaseChannel, Value
from zerograph.errors import EmptyChannelError, InvalidUpdateError

__all__ = ("NamedBarrierValue", "NamedBarrierValueAfterFinish")


class NamedBarrierValue(Generic[Value], BaseChannel[Value, Value, set]):
    """Waits until all named values are received before becoming available."""

    __slots__ = ("names", "seen")

    def __init__(self, typ: type, names: set) -> None:
        super().__init__(typ)
        self.names = set(names)
        self.seen: set = set()

    def __eq__(self, other: object) -> bool:
        return isinstance(other, NamedBarrierValue) and other.names == self.names

    def __hash__(self) -> int:
        return hash(("NamedBarrierValue", frozenset(self.names)))

    @property
    def ValueType(self) -> type:
        return self.typ

    @property
    def UpdateType(self) -> type:
        return self.typ

    def copy(self) -> NamedBarrierValue:
        empty = self.__class__(self.typ, self.names)
        empty.key = self.key
        empty.seen = self.seen.copy()
        return empty

    def checkpoint(self) -> set:
        return self.seen.copy()

    def from_checkpoint(self, checkpoint) -> NamedBarrierValue:
        empty = self.__class__(self.typ, self.names)
        empty.key = self.key
        if checkpoint is not MISSING:
            empty.seen = set(checkpoint)
        return empty

    def update(self, values: Sequence) -> bool:
        updated = False
        for value in values:
            if value in self.names:
                if value not in self.seen:
                    self.seen.add(value)
                    updated = True
            else:
                raise InvalidUpdateError(
                    f"At key '{self.key}': Value {value} not in {self.names}"
                )
        return updated

    def get(self):
        if self.seen != self.names:
            raise EmptyChannelError()
        return None

    def is_available(self) -> bool:
        return self.seen == self.names

    def consume(self) -> bool:
        if self.seen == self.names:
            self.seen = set()
            return True
        return False


class NamedBarrierValueAfterFinish(Generic[Value], BaseChannel[Value, Value, tuple]):
    """Like NamedBarrierValue but only available after finish()."""

    __slots__ = ("names", "seen", "finished")

    def __init__(self, typ: type, names: set) -> None:
        super().__init__(typ)
        self.names = set(names)
        self.seen: set = set()
        self.finished = False

    def __eq__(self, other: object) -> bool:
        return (
            isinstance(other, NamedBarrierValueAfterFinish)
            and other.names == self.names
        )

    def __hash__(self) -> int:
        return hash(("NamedBarrierValueAfterFinish", frozenset(self.names)))

    @property
    def ValueType(self) -> type:
        return self.typ

    @property
    def UpdateType(self) -> type:
        return self.typ

    def copy(self) -> NamedBarrierValueAfterFinish:
        empty = self.__class__(self.typ, self.names)
        empty.key = self.key
        empty.seen = self.seen.copy()
        empty.finished = self.finished
        return empty

    def checkpoint(self) -> tuple:
        return (self.seen.copy(), self.finished)

    def from_checkpoint(self, checkpoint) -> NamedBarrierValueAfterFinish:
        empty = self.__class__(self.typ, self.names)
        empty.key = self.key
        if checkpoint is not MISSING:
            empty.seen, empty.finished = set(checkpoint[0]), checkpoint[1]
        return empty

    def update(self, values: Sequence) -> bool:
        updated = False
        for value in values:
            if value in self.names:
                if value not in self.seen:
                    self.seen.add(value)
                    updated = True
            else:
                raise InvalidUpdateError(
                    f"At key '{self.key}': Value {value} not in {self.names}"
                )
        return updated

    def get(self):
        if not self.finished or self.seen != self.names:
            raise EmptyChannelError()
        return None

    def is_available(self) -> bool:
        return self.finished and self.seen == self.names

    def consume(self) -> bool:
        if self.finished and self.seen == self.names:
            self.finished = False
            self.seen = set()
            return True
        return False

    def finish(self) -> bool:
        if not self.finished and self.seen == self.names:
            self.finished = True
            return True
        return False
