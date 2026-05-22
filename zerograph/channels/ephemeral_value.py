"""EphemeralValue channel - temporary value that clears each step."""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any, Generic

from zerograph._internal import MISSING, _deepcopy_or_warn
from zerograph.channels.base import BaseChannel, Value
from zerograph.errors import EmptyChannelError, InvalidUpdateError

__all__ = ("EphemeralValue",)


class EphemeralValue(Generic[Value], BaseChannel[Value, Value, Value]):
    """Stores value for one step only, clears after."""

    __slots__ = ("value", "guard")

    def __init__(self, typ: Any, guard: bool = True) -> None:
        super().__init__(typ)
        self.guard = guard
        self.value = MISSING

    def __eq__(self, other: object) -> bool:
        return isinstance(other, EphemeralValue) and self.guard == other.guard

    def __hash__(self) -> int:
        return hash(("EphemeralValue", self.guard))

    @property
    def ValueType(self) -> type[Value]:
        return self.typ

    @property
    def UpdateType(self) -> type[Value]:
        return self.typ

    def copy(self) -> EphemeralValue:
        empty = self.__class__(self.typ, self.guard)
        empty.key = self.key
        if self.value is not MISSING:
            empty.value = _deepcopy_or_warn(self.value)
        return empty

    def from_checkpoint(self, checkpoint: Value) -> EphemeralValue:
        empty = self.__class__(self.typ, self.guard)
        empty.key = self.key
        if checkpoint is not MISSING:
            empty.value = _deepcopy_or_warn(checkpoint)
        return empty

    def update(self, values: Sequence[Value]) -> bool:
        if len(values) == 0:
            if self.value is not MISSING:
                self.value = MISSING
                return True
            return False
        if len(values) != 1 and self.guard:
            raise InvalidUpdateError(
                f"At key '{self.key}': EphemeralValue(guard=True) can receive "
                "only one value per step."
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
        if self.value is MISSING:
            return MISSING
        return _deepcopy_or_warn(self.value)
