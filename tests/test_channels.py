"""Unit tests for ZeroGraph channel types."""

import operator
import pytest

from zerograph._internal import MISSING
from zerograph.errors import EmptyChannelError, InvalidUpdateError
from zerograph import Overwrite

from zerograph.channels.last_value import LastValue, LastValueAfterFinish
from zerograph.channels.any_value import AnyValue
from zerograph.channels.ephemeral_value import EphemeralValue
from zerograph.channels.topic import Topic
from zerograph.channels.named_barrier import NamedBarrierValue, NamedBarrierValueAfterFinish
from zerograph.channels.binop import BinaryOperatorAggregate


# ==================== LastValue ====================


class TestLastValue:

    def test_empty_get_raises(self):
        ch = LastValue(int)
        with pytest.raises(EmptyChannelError):
            ch.get()

    def test_update_single_value(self):
        ch = LastValue(int)
        assert ch.update([42]) is True
        assert ch.get() == 42

    def test_update_multiple_raises(self):
        ch = LastValue(int)
        with pytest.raises(InvalidUpdateError):
            ch.update([1, 2])

    def test_update_empty_returns_false(self):
        ch = LastValue(int)
        assert ch.update([]) is False

    def test_is_available(self):
        ch = LastValue(int)
        assert ch.is_available() is False
        ch.update([10])
        assert ch.is_available() is True

    def test_copy_independence(self):
        ch = LastValue(list)
        ch.update([[1, 2]])
        cp = ch.copy()
        cp.value.append(99)
        assert ch.get() == [1, 2]

    def test_checkpoint_missing_when_empty(self):
        ch = LastValue(int)
        assert ch.checkpoint() is MISSING

    def test_checkpoint_returns_deepcopy(self):
        ch = LastValue(list)
        ch.update([[1, 2]])
        cp = ch.checkpoint()
        assert cp == [1, 2]
        cp.append(99)
        assert ch.get() == [1, 2]

    def test_from_checkpoint_restores(self):
        ch = LastValue(int)
        restored = ch.from_checkpoint(42)
        assert restored.get() == 42

    def test_from_checkpoint_missing(self):
        ch = LastValue(int)
        restored = ch.from_checkpoint(MISSING)
        assert restored.is_available() is False

    def test_consume_returns_false(self):
        ch = LastValue(int)
        ch.update([1])
        assert ch.consume() is False

    def test_finish_returns_false(self):
        ch = LastValue(int)
        ch.update([1])
        assert ch.finish() is False

    def test_eq_hash(self):
        a = LastValue(int)
        b = LastValue(str)
        assert a == b
        assert hash(a) == hash(b)


# ==================== LastValueAfterFinish ====================


class TestLastValueAfterFinish:

    def test_get_before_finish_raises(self):
        ch = LastValueAfterFinish(int)
        ch.update([42])
        with pytest.raises(EmptyChannelError):
            ch.get()

    def test_get_after_finish(self):
        ch = LastValueAfterFinish(int)
        ch.update([42])
        assert ch.finish() is True
        assert ch.get() == 42

    def test_update_resets_finished(self):
        ch = LastValueAfterFinish(int)
        ch.update([1])
        ch.finish()
        assert ch.is_available() is True
        ch.update([2])
        assert ch.is_available() is False
        with pytest.raises(EmptyChannelError):
            ch.get()

    def test_consume_clears(self):
        ch = LastValueAfterFinish(int)
        ch.update([10])
        ch.finish()
        assert ch.consume() is True
        assert ch.is_available() is False

    def test_consume_before_finish_returns_false(self):
        ch = LastValueAfterFinish(int)
        ch.update([10])
        assert ch.consume() is False

    def test_checkpoint_tuple(self):
        ch = LastValueAfterFinish(int)
        ch.update([7])
        ch.finish()
        cp = ch.checkpoint()
        assert isinstance(cp, tuple)
        assert cp[0] == 7
        assert cp[1] is True

    def test_checkpoint_missing_when_empty(self):
        ch = LastValueAfterFinish(int)
        assert ch.checkpoint() is MISSING

    def test_from_checkpoint_restores_finished(self):
        ch = LastValueAfterFinish(int)
        restored = ch.from_checkpoint((99, True))
        assert restored.is_available() is True
        assert restored.get() == 99

    def test_finish_returns_false_when_empty(self):
        ch = LastValueAfterFinish(int)
        assert ch.finish() is False

    def test_finish_returns_false_when_already_finished(self):
        ch = LastValueAfterFinish(int)
        ch.update([1])
        assert ch.finish() is True
        assert ch.finish() is False


# ==================== AnyValue ====================


class TestAnyValue:

    def test_multiple_writes_ok(self):
        ch = AnyValue(int)
        ch.update([1, 2, 3])
        assert ch.get() == 3

    def test_basic_get_set(self):
        ch = AnyValue(str)
        assert ch.update(["hello"]) is True
        assert ch.get() == "hello"

    def test_empty_update_returns_false(self):
        ch = AnyValue(int)
        assert ch.update([]) is False

    def test_empty_get_raises(self):
        ch = AnyValue(int)
        with pytest.raises(EmptyChannelError):
            ch.get()

    def test_copy_independence(self):
        ch = AnyValue(list)
        ch.update([[1]])
        cp = ch.copy()
        cp.value.append(2)
        assert ch.get() == [1]

    def test_checkpoint_missing_when_empty(self):
        ch = AnyValue(int)
        assert ch.checkpoint() is MISSING


# ==================== EphemeralValue ====================


class TestEphemeralValue:

    def test_empty_update_clears(self):
        ch = EphemeralValue(int)
        ch.update([42])
        assert ch.is_available() is True
        assert ch.update([]) is True
        assert ch.is_available() is False

    def test_empty_update_no_clear_when_already_empty(self):
        ch = EphemeralValue(int)
        assert ch.update([]) is False

    def test_guard_true_rejects_multiple(self):
        ch = EphemeralValue(int, guard=True)
        with pytest.raises(InvalidUpdateError):
            ch.update([1, 2])

    def test_guard_false_allows_multiple(self):
        ch = EphemeralValue(int, guard=False)
        ch.update([1, 2, 3])
        assert ch.get() == 3

    def test_guard_true_single_value_ok(self):
        ch = EphemeralValue(int, guard=True)
        ch.update([42])
        assert ch.get() == 42

    def test_eq_includes_guard(self):
        a = EphemeralValue(int, guard=True)
        b = EphemeralValue(int, guard=False)
        assert a != b

    def test_hash_includes_guard(self):
        a = EphemeralValue(int, guard=True)
        b = EphemeralValue(int, guard=False)
        assert hash(a) != hash(b)

    def test_checkpoint_missing_when_empty(self):
        ch = EphemeralValue(int)
        assert ch.checkpoint() is MISSING


# ==================== Topic ====================


class TestTopic:

    def test_accumulate_false_replaces(self):
        ch = Topic(int)
        ch.update([1, 2])
        assert ch.get() == [1, 2]
        ch.update([3])
        assert ch.get() == [3]

    def test_accumulate_true_appends(self):
        ch = Topic(int, accumulate=True)
        ch.update([1])
        assert ch.get() == [1]
        ch.update([2, 3])
        assert ch.get() == [1, 2, 3]

    def test_empty_update_clears_when_not_accumulate(self):
        ch = Topic(int)
        ch.update([1])
        assert ch.update([]) is True
        assert ch.is_available() is False

    def test_empty_update_noop_when_accumulate(self):
        ch = Topic(int, accumulate=True)
        ch.update([1])
        assert ch.update([]) is False
        assert ch.get() == [1]

    def test_flatten_nested(self):
        ch = Topic(int)
        ch.update([[1, 2], 3])
        assert ch.get() == [1, 2, 3]

    def test_consume_clears_when_not_accumulate(self):
        ch = Topic(int)
        ch.update([1, 2])
        assert ch.consume() is True
        assert ch.is_available() is False

    def test_consume_noop_when_accumulate(self):
        ch = Topic(int, accumulate=True)
        ch.update([1])
        assert ch.consume() is False

    def test_empty_checkpoint_returns_missing(self):
        ch = Topic(int)
        assert ch.checkpoint() is MISSING

    def test_from_checkpoint_restores(self):
        ch = Topic(int)
        restored = ch.from_checkpoint([10, 20])
        assert restored.get() == [10, 20]

    def test_empty_get_raises(self):
        ch = Topic(int)
        with pytest.raises(EmptyChannelError):
            ch.get()

    def test_eq_includes_accumulate(self):
        a = Topic(int, accumulate=False)
        b = Topic(int, accumulate=True)
        assert a != b


# ==================== NamedBarrierValue ====================


class TestNamedBarrierValue:

    def test_partial_not_available(self):
        ch = NamedBarrierValue(str, {"a", "b"})
        ch.update(["a"])
        assert ch.is_available() is False
        with pytest.raises(EmptyChannelError):
            ch.get()

    def test_all_names_available(self):
        ch = NamedBarrierValue(str, {"a", "b"})
        ch.update(["a"])
        ch.update(["b"])
        assert ch.is_available() is True
        assert ch.get() is None

    def test_unknown_name_raises(self):
        ch = NamedBarrierValue(str, {"a"})
        with pytest.raises(InvalidUpdateError):
            ch.update(["x"])

    def test_consume_resets(self):
        ch = NamedBarrierValue(str, {"a"})
        ch.update(["a"])
        assert ch.consume() is True
        assert ch.is_available() is False

    def test_consume_before_complete_returns_false(self):
        ch = NamedBarrierValue(str, {"a", "b"})
        ch.update(["a"])
        assert ch.consume() is False

    def test_checkpoint_missing_when_empty(self):
        ch = NamedBarrierValue(str, {"a"})
        assert ch.checkpoint() is MISSING

    def test_checkpoint_returns_seen(self):
        ch = NamedBarrierValue(str, {"a", "b"})
        ch.update(["a"])
        cp = ch.checkpoint()
        assert cp == {"a"}

    def test_from_checkpoint_restores(self):
        ch = NamedBarrierValue(str, {"a", "b"})
        restored = ch.from_checkpoint({"a"})
        assert restored.seen == {"a"}

    def test_duplicate_value_idempotent(self):
        ch = NamedBarrierValue(str, {"a"})
        assert ch.update(["a"]) is True
        assert ch.update(["a"]) is False

    def test_eq_includes_names(self):
        a = NamedBarrierValue(str, {"x"})
        b = NamedBarrierValue(str, {"y"})
        assert a != b

    def test_hash_includes_names(self):
        a = NamedBarrierValue(str, {"x"})
        b = NamedBarrierValue(str, {"y"})
        assert hash(a) != hash(b)


# ==================== NamedBarrierValueAfterFinish ====================


class TestNamedBarrierValueAfterFinish:

    def test_requires_finish(self):
        ch = NamedBarrierValueAfterFinish(str, {"a"})
        ch.update(["a"])
        assert ch.is_available() is False
        ch.finish()
        assert ch.is_available() is True

    def test_get_before_finish_raises(self):
        ch = NamedBarrierValueAfterFinish(str, {"a"})
        ch.update(["a"])
        with pytest.raises(EmptyChannelError):
            ch.get()

    def test_update_resets_finished(self):
        ch = NamedBarrierValueAfterFinish(str, {"a"})
        ch.update(["a"])
        ch.finish()
        assert ch.is_available() is True
        ch.update(["a"])
        assert ch.is_available() is False

    def test_consume_resets(self):
        ch = NamedBarrierValueAfterFinish(str, {"a"})
        ch.update(["a"])
        ch.finish()
        assert ch.consume() is True
        assert ch.is_available() is False

    def test_checkpoint_always_tuple(self):
        ch = NamedBarrierValueAfterFinish(str, {"a"})
        cp = ch.checkpoint()
        assert isinstance(cp, tuple)
        assert cp == (set(), False)


# ==================== BinaryOperatorAggregate ====================


class TestBinaryOperatorAggregate:

    def test_operator_applied(self):
        ch = BinaryOperatorAggregate(int, operator.add)
        ch.update([1])
        ch.update([2])
        assert ch.get() == 3

    def test_operator_list_append(self):
        ch = BinaryOperatorAggregate(list, operator.add)
        ch.update([[1]])
        ch.update([[2]])
        assert ch.get() == [1, 2]

    def test_overwrite_resets(self):
        ch = BinaryOperatorAggregate(int, operator.add)
        ch.update([10])
        ch.update([Overwrite(5)])
        assert ch.get() == 5

    def test_double_overwrite_raises(self):
        ch = BinaryOperatorAggregate(int, operator.add)
        with pytest.raises(InvalidUpdateError):
            ch.update([Overwrite(1), Overwrite(2)])

    def test_default_constructible_type(self):
        ch = BinaryOperatorAggregate(list, operator.add)
        assert ch.is_available() is True
        assert ch.get() == []

    def test_constructible_type_starts_with_default(self):
        ch = BinaryOperatorAggregate(int, operator.add)
        assert ch.is_available() is True
        assert ch.get() == 0

    def test_empty_update_returns_false(self):
        ch = BinaryOperatorAggregate(int, operator.add)
        assert ch.update([]) is False

    def test_batch_update(self):
        ch = BinaryOperatorAggregate(int, operator.add)
        ch.update([1, 2, 3])
        assert ch.get() == 6

    def test_overwrite_dict_form(self):
        ch = BinaryOperatorAggregate(int, operator.add)
        ch.update([10])
        ch.update([{"__overwrite__": 0}])
        assert ch.get() == 0

    def test_copy_independence(self):
        ch = BinaryOperatorAggregate(list, operator.add)
        ch.update([[1]])
        cp = ch.copy()
        cp.value.append(99)
        assert ch.get() == [1]

    def test_from_checkpoint_restores(self):
        ch = BinaryOperatorAggregate(int, operator.add)
        restored = ch.from_checkpoint(42)
        assert restored.get() == 42

    def test_eq_operator_identity(self):
        a = BinaryOperatorAggregate(int, operator.add)
        b = BinaryOperatorAggregate(int, operator.add)
        assert a == b

    def test_eq_different_operator(self):
        a = BinaryOperatorAggregate(int, operator.add)
        b = BinaryOperatorAggregate(int, operator.mul)
        assert a != b
