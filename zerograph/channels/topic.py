"""Topic channel - pub/sub pattern for broadcasting values."""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any, Generic

from zerograph._internal import MISSING
from zerograph.channels.base import BaseChannel, Value
from zerograph.errors import EmptyChannelError

__all__ = ("Topic",)


def _flatten(values: Sequence) -> list:
    result = []
    for v in values:
        if isinstance(v, list):
            result.extend(v)
        else:
            result.append(v)
    return result


class Topic(Generic[Value], BaseChannel[list[Value], Value, list[Value]]):
    """Pub/sub topic channel for broadcasting messages."""

    __slots__ = ("values", "accumulate")

    def __init__(self, typ: Any, accumulate: bool = False, key: str = "") -> None:
        super().__init__(typ, key)
        self.accumulate = accumulate
        self.values: list = []

    def __eq__(self, other: object) -> bool:
        return isinstance(other, Topic) and other.accumulate == self.accumulate

    def __hash__(self) -> int:
        return hash(("Topic", self.accumulate))

    @property
    def ValueType(self) -> type:
        return list

    @property
    def UpdateType(self) -> type:
        return self.typ

    def copy(self) -> Topic:
        empty = self.__class__(self.typ, self.accumulate, self.key)
        empty.values = self.values.copy()
        return empty

    def from_checkpoint(self, checkpoint) -> Topic:
        empty = self.__class__(self.typ, self.accumulate, self.key)
        if checkpoint is not MISSING:
            empty.values = checkpoint
        return empty

    def update(self, values: Sequence[Value]) -> bool:
        if not values:
            if not self.accumulate and self.values:
                self.values = []
                return True
            return False
        flat = _flatten(values)
        if self.accumulate:
            self.values.extend(flat)
        else:
            self.values = flat
        return True

    def get(self) -> list[Value]:
        if not self.values:
            raise EmptyChannelError()
        return self.values

    def is_available(self) -> bool:
        return bool(self.values)

    def checkpoint(self):
        return self.values.copy()

    def consume(self) -> bool:
        if not self.accumulate and self.values:
            self.values = []
            return True
        return False
