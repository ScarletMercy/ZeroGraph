"""BinaryOperatorAggregate channel - applies a reducer function."""

from __future__ import annotations

import collections.abc
from collections.abc import Callable, Sequence
from typing import Any, Generic

from zerograph._internal import MISSING
from zerograph.channels.base import BaseChannel, Value
from zerograph.errors import EmptyChannelError, InvalidUpdateError
from zerograph.types import Overwrite
from zerograph.constants import OVERWRITE


def _strip_extras(t):
    if hasattr(t, "__origin__"):
        return _strip_extras(t.__origin__)
    return t


class BinaryOperatorAggregate(Generic[Value], BaseChannel[Value, Value, Value]):
    """Stores the result of applying a binary operator (reducer) to values."""

    __slots__ = ("value", "operator")

    def __init__(self, typ: type[Value], operator: Callable[[Value, Value], Value]):
        super().__init__(typ)
        self.operator = operator
        typ = _strip_extras(typ)
        if typ in (collections.abc.Sequence, collections.abc.MutableSequence):
            typ = list
        if typ in (collections.abc.Set, collections.abc.MutableSet):
            typ = set
        if typ in (collections.abc.Mapping, collections.abc.MutableMapping):
            typ = dict
        try:
            self.value = typ()
        except Exception:
            self.value = MISSING

    def __eq__(self, other: object) -> bool:
        return isinstance(other, BinaryOperatorAggregate) and self.operator == other.operator

    def __hash__(self) -> int:
        return hash(("BinaryOperatorAggregate",))

    @property
    def ValueType(self) -> type[Value]:
        return self.typ

    @property
    def UpdateType(self) -> type[Value]:
        return self.typ

    def copy(self) -> BinaryOperatorAggregate:
        empty = self.__class__(self.typ, self.operator)
        empty.key = self.key
        empty.value = self.value
        return empty

    def from_checkpoint(self, checkpoint: Value) -> BinaryOperatorAggregate:
        empty = self.__class__(self.typ, self.operator)
        empty.key = self.key
        if checkpoint is not MISSING:
            empty.value = checkpoint
        return empty

    def update(self, values: Sequence[Value]) -> bool:
        if not values:
            return False
        if self.value is MISSING:
            first = values[0]
            is_ow, ow_val = _get_overwrite(first)
            self.value = ow_val if is_ow else first
            values = values[1:]
        seen_overwrite = False
        for value in values:
            is_overwrite, overwrite_value = _get_overwrite(value)
            if is_overwrite:
                if seen_overwrite:
                    raise InvalidUpdateError(
                        "Can receive only one Overwrite value per super-step."
                    )
                self.value = overwrite_value
                seen_overwrite = True
                continue
            self.value = self.operator(self.value, value)
        return True

    def get(self) -> Value:
        if self.value is MISSING:
            raise EmptyChannelError()
        return self.value

    def is_available(self) -> bool:
        return self.value is not MISSING

    def checkpoint(self) -> Value:
        return self.value


def _get_overwrite(value: Any) -> tuple[bool, Any]:
    if isinstance(value, Overwrite):
        return True, value.value
    if isinstance(value, dict) and len(value) == 1 and OVERWRITE in value:
        return True, value[OVERWRITE]
    return False, None
