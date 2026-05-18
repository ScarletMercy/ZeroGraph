"""AnyValue channel - stores last value, accepts multiple writes per step."""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any, Generic

from zerograph._internal import MISSING
from zerograph.channels.base import BaseChannel, Value
from zerograph.errors import EmptyChannelError

__all__ = ("AnyValue",)


class AnyValue(Generic[Value], BaseChannel[Value, Value, Value]):
    """Like LastValue but accepts multiple values per step without error.

    Silently keeps the last value received. Useful when multiple nodes
    write the same value in a superstep (e.g., context injection).
    """

    __slots__ = ("value",)

    def __init__(self, typ: Any, key: str = "") -> None:
        super().__init__(typ, key)
        self.value = MISSING

    def __eq__(self, other: object) -> bool:
        return isinstance(other, AnyValue)

    def __hash__(self) -> int:
        return hash(("AnyValue",))

    @property
    def ValueType(self) -> type[Value]:
        return self.typ

    @property
    def UpdateType(self) -> type[Value]:
        return self.typ

    def copy(self) -> AnyValue:
        empty = self.__class__(self.typ, self.key)
        empty.value = self.value
        return empty

    def from_checkpoint(self, checkpoint: Value) -> AnyValue:
        empty = self.__class__(self.typ, self.key)
        if checkpoint is not MISSING:
            empty.value = checkpoint
        return empty

    def update(self, values: Sequence[Value]) -> bool:
        if not values:
            return False
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
