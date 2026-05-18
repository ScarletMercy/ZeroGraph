"""s05_send — 动态路由 Send/Overwrite/Command(goto) + 通道类型测试。"""

from typing import Annotated, TypedDict
import operator

from zerograph import (
    START,
    END,
    StateGraph,
    InMemorySaver,
    Send,
    Command,
    Overwrite,
    AnyValue,
)
from zerograph.channels.ephemeral_value import EphemeralValue
from zerograph.channels.last_value import LastValue, LastValueAfterFinish
from zerograph.channels.binop import BinaryOperatorAggregate
from zerograph.channels.named_barrier import (
    NamedBarrierValue,
    NamedBarrierValueAfterFinish,
)
from zerograph.channels.topic import Topic
from zerograph.channels.base import BaseChannel


# ---- Schemas ----

class _ListSt(TypedDict):
    items: Annotated[list, operator.add]


class _CountSt(TypedDict):
    count: Annotated[int, operator.add]


class _GotoSt(TypedDict):
    x: int
    y: int


# ---- Test 1: Send fan-out ----

def _test_send_fanout() -> tuple[str, bool, str]:
    """Conditional edge returns Send() objects; node runs once per Send."""
    try:

        def router(state):
            return [Send("process", "item1"), Send("process", "item2")]

        def process(state):
            if isinstance(state, dict):
                tag = state.get("items", ["unknown"])[-1] if state.get("items") else str(state)
            else:
                tag = state
            return {"items": [f"processed_{tag}"]}

        g = StateGraph(_ListSt)
        g.add_node("router", router)
        g.add_node("process", process)
        g.add_edge(START, "router")
        g.add_conditional_edges("router", router, path_map={"process": "process"})
        g.add_edge("process", END)
        app = g.compile()

        result = app.invoke({"items": []})
        processed = result.get("items", [])
        ok1 = "processed_item1" in processed
        ok2 = "processed_item2" in processed
        if ok1 and ok2:
            return ("Send fan-out", True, f"Fan-out processed both items: {processed}")
        return ("Send fan-out", False, f"Expected both items processed, got {processed}")
    except Exception as e:
        return ("Send fan-out", False, str(e))


# ---- Test 2: Overwrite ----

def _test_overwrite() -> tuple[str, bool, str]:
    """Overwrite bypasses reducer and sets value directly."""
    try:

        def accum(state):
            return {"count": 5}

        def overwrite_node(state):
            return {"count": Overwrite(100)}

        g = StateGraph(_CountSt)
        g.add_node("accum", accum)
        g.add_node("overwrite_node", overwrite_node)
        g.add_edge(START, "accum")
        g.add_edge("accum", "overwrite_node")
        g.add_edge("overwrite_node", END)
        app = g.compile()

        result = app.invoke({"count": 0})
        if result.get("count") == 100:
            return ("Overwrite", True, "Overwrite bypassed reducer, count=100")
        return ("Overwrite", False, f"Expected count=100, got {result.get('count')}")
    except Exception as e:
        return ("Overwrite", False, str(e))


# ---- Test 3: Command(goto=...) ----

def _test_command_goto() -> tuple[str, bool, str]:
    """Command(goto=...) redirects flow, skipping normal edge routing."""
    try:

        def node_a(state):
            return Command(update={"x": 1, "y": 0}, goto="node_c")

        def node_b(state):
            return {"x": state["x"] + 10, "y": state["y"] + 10}

        def node_c(state):
            return {"x": state["x"] + 100, "y": state["y"] + 100}

        g = StateGraph(_GotoSt)
        g.add_node("node_a", node_a)
        g.add_node("node_b", node_b)
        g.add_node("node_c", node_c)
        g.add_edge(START, "node_a")
        g.add_edge("node_a", "node_b")
        g.add_edge("node_b", END)
        g.add_edge("node_c", END)
        app = g.compile()

        result = app.invoke({"x": 0, "y": 0})
        if result.get("x") == 101 and result.get("y") == 100:
            return ("Command(goto)", True, f"goto skipped node_b: x={result['x']}, y={result['y']}")
        return ("Command(goto)", False, f"Expected x=101, y=100; got {result}")
    except Exception as e:
        return ("Command(goto)", False, str(e))


# ---- Test 4: EphemeralValue ----

def _test_ephemeral_value() -> tuple[str, bool, str]:
    """EphemeralValue stores value and clears on empty update."""
    try:
        ch = EphemeralValue(int, guard=True)
        ch.key = "test_eph"
        if ch.is_available():
            return ("EphemeralValue", False, "should be empty initially")
        ch.update([42])
        if not ch.is_available():
            return ("EphemeralValue", False, "should be available after update")
        if ch.get() != 42:
            return ("EphemeralValue", False, f"Expected 42, got {ch.get()}")
        ch.update([])
        if ch.is_available():
            return ("EphemeralValue", False, "should be cleared after empty update")
        return ("EphemeralValue", True, "EphemeralValue update/get/clear works correctly")
    except Exception as e:
        return ("EphemeralValue", False, str(e))


# ---- Test 5: AnyValue ----

def _test_any_value() -> tuple[str, bool, str]:
    """AnyValue accepts multiple writes per step, keeps last."""
    try:
        ch = AnyValue(int, "test_any")
        if ch.is_available():
            return ("AnyValue", False, "should be empty initially")
        ch.update([10, 20, 30])
        if not ch.is_available():
            return ("AnyValue", False, "should be available after update")
        if ch.get() != 30:
            return ("AnyValue", False, f"Expected last value 30, got {ch.get()}")
        ch.update([99])
        if ch.get() != 99:
            return ("AnyValue", False, f"Expected 99, got {ch.get()}")
        return ("AnyValue", True, "AnyValue handles multiple writes, keeps last value")
    except Exception as e:
        return ("AnyValue", False, str(e))


# ---------------------------------------------------------------------------
# Test 6: LastValue + EmptyChannelError
# ---------------------------------------------------------------------------

def _test_last_value() -> tuple[str, bool, str]:
    from zerograph.errors import EmptyChannelError

    ch = LastValue(int, "lv_test")
    if ch.is_available():
        return ("LastValue", False, "should be empty initially")
    try:
        ch.get()
        return ("LastValue", False, "get() on empty should raise EmptyChannelError")
    except EmptyChannelError:
        pass
    ch.update([42])
    if ch.get() != 42:
        return ("LastValue", False, f"expected 42, got {ch.get()}")
    cp = ch.checkpoint()
    ch2 = ch.from_checkpoint(cp)
    if ch2.get() != 42:
        return ("LastValue", False, f"checkpoint restore failed: {ch2.get()}")
    return ("LastValue", True, "update/get/checkpoint works, EmptyChannelError raised")


# ---------------------------------------------------------------------------
# Test 7: LastValueAfterFinish
# ---------------------------------------------------------------------------

def _test_last_value_after_finish() -> tuple[str, bool, str]:
    from zerograph.errors import EmptyChannelError

    ch = LastValueAfterFinish(int, "lvaf_test")
    ch.update([99])
    # Not finished yet — get() should raise
    try:
        ch.get()
        return ("LastValueAfterFinish", False, "get() before finish should raise")
    except EmptyChannelError:
        pass
    ch.finish()
    if ch.get() != 99:
        return ("LastValueAfterFinish", False, f"expected 99, got {ch.get()}")
    ch.consume()
    if ch.is_available():
        return ("LastValueAfterFinish", False, "should be unavailable after consume")
    return ("LastValueAfterFinish", True, "finish/get/consume cycle works")


# ---------------------------------------------------------------------------
# Test 8: BinaryOperatorAggregate
# ---------------------------------------------------------------------------

def _test_binop_aggregate() -> tuple[str, bool, str]:
    ch = BinaryOperatorAggregate(int, operator.add)
    ch.key = "bop_test"
    ch.update([10])
    ch.update([20])
    if ch.get() != 30:
        return ("BinaryOperatorAggregate", False, f"expected 30, got {ch.get()}")
    ch2 = ch.from_checkpoint(ch.checkpoint())
    if ch2.get() != 30:
        return ("BinaryOperatorAggregate", False, f"checkpoint restore: {ch2.get()}")
    return ("BinaryOperatorAggregate", True, "0+10+20=30, checkpoint works")


# ---------------------------------------------------------------------------
# Test 9: NamedBarrierValue
# ---------------------------------------------------------------------------

def _test_named_barrier() -> tuple[str, bool, str]:
    from zerograph.errors import EmptyChannelError

    ch = NamedBarrierValue(str, {"a", "b"})
    ch.key = "nb_test"
    ch.update(["a"])
    if ch.is_available():
        return ("NamedBarrierValue", False, "should wait for all names")
    ch.update(["b"])
    if not ch.is_available():
        return ("NamedBarrierValue", False, "should be available when all names seen")
    # get() returns None (barrier signals completion)
    ch.get()
    ch.consume()
    if ch.is_available():
        return ("NamedBarrierValue", False, "should be unavailable after consume")
    return ("NamedBarrierValue", True, "barrier collects a+b then consume clears")


# ---------------------------------------------------------------------------
# Test 10: Topic
# ---------------------------------------------------------------------------

def _test_topic() -> tuple[str, bool, str]:
    ch = Topic(str, key="topic_test")
    ch.update(["hello", "world"])
    vals = ch.get()
    if vals != ["hello", "world"]:
        return ("Topic", False, f"expected ['hello','world'], got {vals}")
    ch.consume()
    if ch.is_available():
        return ("Topic", False, "should be empty after consume (non-accumulate)")
    # Accumulate mode
    ch2 = Topic(str, accumulate=True, key="topic_acc")
    ch2.update(["a"])
    ch2.update(["b"])
    if ch2.get() != ["a", "b"]:
        return ("Topic", False, f"accumulate mode: {ch2.get()}")
    return ("Topic", True, "pub/sub + accumulate works")


# ---------------------------------------------------------------------------
# Test 11: BaseChannel ABC
# ---------------------------------------------------------------------------

def _test_base_channel() -> tuple[str, bool, str]:
    ok = (
        hasattr(BaseChannel, "update")
        and hasattr(BaseChannel, "get")
        and hasattr(BaseChannel, "checkpoint")
        and hasattr(BaseChannel, "from_checkpoint")
    )
    # Verify concrete channels are subclasses
    ok = ok and issubclass(LastValue, BaseChannel)
    ok = ok and issubclass(Topic, BaseChannel)
    return ("BaseChannel ABC", ok, f"abstract methods + subclass checks: {ok}")


# ---------------------------------------------------------------------------
# Test 12: Command(goto=...) + Send 组合 — goto 跳过节点时 Send 队列清理
# ---------------------------------------------------------------------------

def _test_goto_with_send() -> tuple[str, bool, str]:
    """When a node uses Command(goto=...) to jump forward, pending Sends from
    earlier edges must not leak into subsequent steps."""
    try:

        # Graph: START → router (sends to "worker") → worker → judge → END
        #                                                         ↑
        #   judge uses Command(goto="judge") to re-enter itself.
        #   If Send queue is not cleaned, the old Send("worker", ...) will
        #   re-execute worker on the second pass, corrupting the result.

        execution_log: list[str] = []

        def router(state):
            execution_log.append("router")
            return [Send("worker", i) for i in range(3)]

        def worker(state):
            tag = state if not isinstance(state, dict) else state.get("x", "?")
            execution_log.append(f"worker({tag})")
            return {"items": [f"w{tag}"]}

        call_count = {"n": 0}

        def judge(state):
            call_count["n"] += 1
            execution_log.append(f"judge({call_count['n']})")
            if call_count["n"] < 2:
                # goto self — Send queue should NOT re-trigger worker
                return Command(goto="judge", update={"x": state.get("x", 0) + 1})
            return {"x": state.get("x", 0) + 100}

        g = StateGraph(_ListSt)
        g.add_node("router", router)
        g.add_node("worker", worker)
        g.add_node("judge", judge)
        g.add_edge(START, "router")
        g.add_conditional_edges("router", router, path_map={"worker": "worker"})
        g.add_edge("worker", "judge")
        g.add_edge("judge", END)
        app = g.compile()

        result = app.invoke({"items": [], "x": 0})
        worker_count = sum(1 for e in execution_log if e.startswith("worker("))

        # worker should execute exactly 3 times (one per Send), never more
        if worker_count != 3:
            return ("Command(goto)+Send", False,
                    f"worker ran {worker_count} times (expected 3), "
                    f"Send queue leaked on goto. log={execution_log}")

        # judge should run exactly 2 times (first goto self, then finish)
        judge_count = sum(1 for e in execution_log if e.startswith("judge("))
        if judge_count != 2:
            return ("Command(goto)+Send", False,
                    f"judge ran {judge_count} times (expected 2). log={execution_log}")

        return ("Command(goto)+Send", True,
                f"worker=3x, judge=2x, no Send leak. result={result}")
    except Exception as e:
        return ("Command(goto)+Send", False, str(e))


# ---------------------------------------------------------------------------
# Runner
# ---------------------------------------------------------------------------

def run() -> list[tuple[str, bool, str]]:
    results: list[tuple[str, bool, str]] = []
    for fn in (
        _test_send_fanout,
        _test_overwrite,
        _test_command_goto,
        _test_ephemeral_value,
        _test_any_value,
        _test_last_value,
        _test_last_value_after_finish,
        _test_binop_aggregate,
        _test_named_barrier,
        _test_topic,
        _test_base_channel,
        _test_goto_with_send,
    ):
        try:
            results.append(fn())
        except Exception as exc:
            results.append((fn.__name__, False, str(exc)))
    return results
