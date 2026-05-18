"""s04_interrupt — 中断与恢复测试。"""

from typing import TypedDict

from zerograph import (
    START,
    END,
    StateGraph,
    InMemorySaver,
    Command,
)


class St(TypedDict):
    value: int


# ---------------------------------------------------------------------------
# Test 1: interrupt() inside a node
# ---------------------------------------------------------------------------

def _test_interrupt_in_node() -> tuple[str, bool, str]:
    from zerograph import interrupt

    def node_a(state):
        val = interrupt("need input")
        return {"value": val}

    saver = InMemorySaver()
    g = StateGraph(St)
    g.add_node("node_a", node_a)
    g.add_edge(START, "node_a")
    g.add_edge("node_a", END)
    app = g.compile(checkpointer=saver)

    cfg = {"configurable": {"thread_id": "t1"}}
    # interrupt() halts execution internally; invoke returns current state
    result1 = app.invoke({"value": 0}, cfg)

    snap = app.get_state(cfg)
    if "node_a" not in snap.next:
        return ("interrupt() in node", False,
                f"Expected next contains 'node_a', got {snap.next}")

    # Resume with value 42
    result = app.invoke(Command(resume=42), cfg)
    if result.get("value") == 42:
        return ("interrupt() in node", True, "halted, resume delivered 42")
    return ("interrupt() in node", False, f"expected 42, got {result}")


# ---------------------------------------------------------------------------
# Test 2: interrupt_before
# ---------------------------------------------------------------------------

def _test_interrupt_before() -> tuple[str, bool, str]:
    def node_a(state):
        return {"value": state["value"] + 1}

    def node_b(state):
        return {"value": state["value"] * 10}

    saver = InMemorySaver()
    g = StateGraph(St)
    g.add_node("node_a", node_a)
    g.add_node("node_b", node_b)
    g.add_edge(START, "node_a")
    g.add_edge("node_a", "node_b")
    g.add_edge("node_b", END)
    app = g.compile(checkpointer=saver, interrupt_before=["node_b"])

    cfg = {"configurable": {"thread_id": "t2"}}
    # node_a runs (0+1=1), then interrupt_before pauses before node_b
    app.invoke({"value": 0}, cfg)

    snap = app.get_state(cfg)
    if "node_b" not in snap.next:
        return ("interrupt_before", False,
                f"Expected next contains 'node_b', got {snap.next}")
    if snap.values.get("value") != 1:
        return ("interrupt_before", False,
                f"Expected value=1, got {snap.values.get('value')}")

    # Resume: node_b runs (1*10=10)
    result = app.invoke(Command(resume=None), cfg)
    if result.get("value") == 10:
        return ("interrupt_before", True, "paused, resumed to value=10")
    return ("interrupt_before", False, f"expected 10, got {result}")


# ---------------------------------------------------------------------------
# Test 3: interrupt_after
# ---------------------------------------------------------------------------

def _test_interrupt_after() -> tuple[str, bool, str]:
    def node_a(state):
        return {"value": state["value"] + 5}

    def node_b(state):
        return {"value": state["value"] + 100}

    saver = InMemorySaver()
    g = StateGraph(St)
    g.add_node("node_a", node_a)
    g.add_node("node_b", node_b)
    g.add_edge(START, "node_a")
    g.add_edge("node_a", "node_b")
    g.add_edge("node_b", END)
    app = g.compile(checkpointer=saver, interrupt_after=["node_a"])

    cfg = {"configurable": {"thread_id": "t3"}}
    app.invoke({"value": 0}, cfg)

    snap = app.get_state(cfg)
    if snap.values.get("value") != 5:
        return ("interrupt_after", False,
                f"Expected value=5, got {snap.values.get('value')}")

    result = app.invoke(Command(resume=None), cfg)
    if result.get("value") == 105:
        return ("interrupt_after", True, "paused at 5, resumed to 105")
    return ("interrupt_after", False, f"expected 105, got {result}")


# ---------------------------------------------------------------------------
# Test 4: Multi-step resume
# ---------------------------------------------------------------------------

def _test_multistep_resume() -> tuple[str, bool, str]:
    from zerograph import interrupt

    def node_a(state):
        return {"value": state["value"] + 1}

    def node_b(state):
        val = interrupt("pause_b")
        return {"value": state["value"] + val}

    def node_c(state):
        return {"value": state["value"] + 1000}

    saver = InMemorySaver()
    g = StateGraph(St)
    g.add_node("node_a", node_a)
    g.add_node("node_b", node_b)
    g.add_node("node_c", node_c)
    g.add_edge(START, "node_a")
    g.add_edge("node_a", "node_b")
    g.add_edge("node_b", "node_c")
    g.add_edge("node_c", END)
    app = g.compile(checkpointer=saver)

    cfg = {"configurable": {"thread_id": "t4"}}
    # A runs (0+1=1), B interrupts
    app.invoke({"value": 0}, cfg)

    snap = app.get_state(cfg)
    if snap.values.get("value") != 1:
        return ("Multi-step resume", False,
                f"After A: expected 1, got {snap.values.get('value')}")

    # Resume with 10: B runs (1+10=11), C runs (11+1000=1011)
    result = app.invoke(Command(resume=10), cfg)
    if result.get("value") == 1011:
        return ("Multi-step resume", True, "1 → 11 → 1011")
    return ("Multi-step resume", False, f"expected 1011, got {result}")


# ---------------------------------------------------------------------------
# Test 5: interrupt_before all explicit nodes
# ---------------------------------------------------------------------------

def _test_interrupt_before_all() -> tuple[str, bool, str]:
    def node_a(state):
        return {"value": state["value"] + 1}

    def node_b(state):
        return {"value": state["value"] * 2}

    saver = InMemorySaver()
    g = StateGraph(St)
    g.add_node("node_a", node_a)
    g.add_node("node_b", node_b)
    g.add_edge(START, "node_a")
    g.add_edge("node_a", "node_b")
    g.add_edge("node_b", END)
    app = g.compile(checkpointer=saver, interrupt_before=["node_a", "node_b"])

    cfg = {"configurable": {"thread_id": "t5"}}
    # First invoke: interrupt before node_a (nothing ran yet)
    app.invoke({"value": 0}, cfg)

    snap = app.get_state(cfg)
    if "node_a" not in snap.next:
        return ("interrupt_before all", False,
                f"Expected next contains 'node_a', got {snap.next}")

    # Resume: node_a runs (0+1=1), then interrupt before node_b
    app.invoke(Command(resume=None), cfg)

    snap2 = app.get_state(cfg)
    if "node_b" not in snap2.next:
        return ("interrupt_before all", False,
                f"Expected next contains 'node_b', got {snap2.next}")
    if snap2.values.get("value") != 1:
        return ("interrupt_before all", False,
                f"After node_a: expected 1, got {snap2.values.get('value')}")

    # Resume: node_b runs (1*2=2)
    result = app.invoke(Command(resume=None), cfg)
    if result.get("value") == 2:
        return ("interrupt_before all", True, "step-by-step: 0 → 1 → 2")
    return ("interrupt_before all", False, f"expected 2, got {result}")


# ---------------------------------------------------------------------------
# Runner
# ---------------------------------------------------------------------------

def run() -> list[tuple[str, bool, str]]:
    results: list[tuple[str, bool, str]] = []
    for fn in (
        _test_interrupt_in_node,
        _test_interrupt_before,
        _test_interrupt_after,
        _test_multistep_resume,
        _test_interrupt_before_all,
    ):
        try:
            results.append(fn())
        except Exception as exc:
            results.append((fn.__name__, False, str(exc)))
    return results
