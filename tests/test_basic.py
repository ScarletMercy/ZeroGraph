"""Tests for ZeroGraph core functionality."""

import asyncio
import pytest
from typing import Annotated
from typing_extensions import TypedDict
import operator

from zerograph import (
    StateGraph,
    START,
    END,
    Send,
    Command,
    Interrupt,
    InMemorySaver,
    interrupt,
    GraphRecursionError,
    Overwrite,
    RetryPolicy,
)


# ---- 1. Simple linear graph ----

class SimpleState(TypedDict):
    value: int


def add_one(state: SimpleState) -> dict:
    return {"value": state["value"] + 1}


def multiply_two(state: SimpleState) -> dict:
    return {"value": state["value"] * 2}


def test_linear_graph():
    """Test a simple linear graph: START -> A -> B -> END"""
    graph = StateGraph(SimpleState)
    graph.add_node("add", add_one)
    graph.add_node("mul", multiply_two)
    graph.add_edge(START, "add")
    graph.add_edge("add", "mul")
    graph.add_edge("mul", END)
    compiled = graph.compile()

    result = compiled.invoke({"value": 3})
    assert result == {"value": 8}  # (3+1)*2 = 8


def test_single_node_graph():
    """Test a graph with a single node."""
    graph = StateGraph(SimpleState)
    graph.add_node("inc", add_one)
    graph.add_edge(START, "inc")
    graph.add_edge("inc", END)
    compiled = graph.compile()

    result = compiled.invoke({"value": 0})
    assert result == {"value": 1}


# ---- 2. Reducers (BinaryOperatorAggregate) ----

class ListState(TypedDict):
    items: Annotated[list, operator.add]


def add_item(state: ListState) -> dict:
    return {"items": ["new_item"]}


def add_another(state: ListState) -> dict:
    return {"items": ["another"]}


def test_reducer():
    """Test state with reducer (BinaryOperatorAggregate)."""
    graph = StateGraph(ListState)
    graph.add_node("add", add_item)
    graph.add_node("add2", add_another)
    graph.add_edge(START, "add")
    graph.add_edge("add", "add2")
    graph.add_edge("add2", END)
    compiled = graph.compile()

    result = compiled.invoke({"items": ["initial"]})
    assert result == {"items": ["initial", "new_item", "another"]}


# ---- 3. Conditional edges ----

class CondState(TypedDict):
    value: int
    path: str


def router(state: CondState) -> str:
    if state["value"] > 5:
        return "high"
    return "low"


def high_node(state: CondState) -> dict:
    return {"path": "high"}


def low_node(state: CondState) -> dict:
    return {"path": "low"}


def test_conditional_edges():
    """Test conditional edges."""
    graph = StateGraph(CondState)
    graph.add_node("router_node", lambda s: s)
    graph.add_node("high", high_node)
    graph.add_node("low", low_node)
    graph.add_edge(START, "router_node")
    graph.add_conditional_edges("router_node", router, {"high": "high", "low": "low"})
    graph.add_edge("high", END)
    graph.add_edge("low", END)

    compiled = graph.compile()

    result_high = compiled.invoke({"value": 10, "path": ""})
    assert result_high["path"] == "high"

    result_low = compiled.invoke({"value": 2, "path": ""})
    assert result_low["path"] == "low"


# ---- 4. Fan-out with Send ----

class MapState(TypedDict):
    subjects: list[str]
    results: Annotated[list, operator.add]


def fan_out(state: MapState) -> list[Send]:
    return [Send("process", {"subject": s}) for s in state["subjects"]]


def process_subject(state: dict) -> dict:
    return {"results": [f"Processed: {state['subject']}"]}


def test_send_fan_out():
    """Test fan-out using Send."""
    graph = StateGraph(MapState)
    graph.add_node("fan_out", fan_out)
    graph.add_node("process", process_subject)
    graph.add_conditional_edges(START, fan_out)
    graph.add_edge("process", END)

    compiled = graph.compile()
    result = compiled.invoke({"subjects": ["a", "b", "c"], "results": []})
    assert len(result["results"]) == 3
    assert "Processed: a" in result["results"]


# ---- 5. Streaming ----

def test_stream_updates():
    """Test streaming with updates mode."""
    graph = StateGraph(SimpleState)
    graph.add_node("add", add_one)
    graph.add_node("mul", multiply_two)
    graph.add_edge(START, "add")
    graph.add_edge("add", "mul")
    graph.add_edge("mul", END)
    compiled = graph.compile()

    updates = list(compiled.stream({"value": 3}, stream_mode="updates"))
    assert len(updates) >= 2


def test_stream_values():
    """Test streaming with values mode."""
    graph = StateGraph(SimpleState)
    graph.add_node("add", add_one)
    graph.add_edge(START, "add")
    graph.add_edge("add", END)
    compiled = graph.compile()

    values = list(compiled.stream({"value": 5}, stream_mode="values"))
    assert len(values) >= 1
    assert values[-1] == {"value": 6}


# ---- 6. Recursion limit ----

def test_recursion_limit():
    """Test that recursion limit is enforced."""
    graph = StateGraph(SimpleState)
    graph.add_node("loop", lambda s: {"value": s["value"] + 1})
    graph.add_edge(START, "loop")
    graph.add_edge("loop", "loop")

    compiled = graph.compile()

    with pytest.raises(GraphRecursionError):
        compiled.invoke({"value": 0}, {"recursion_limit": 5})


# ---- 7. Overwrite ----

class OverwriteState(TypedDict):
    items: Annotated[list, operator.add]


def test_overwrite():
    """Test Overwrite to bypass reducer."""
    graph = StateGraph(OverwriteState)
    graph.add_node("reset", lambda s: {"items": Overwrite(value=["reset"])})
    graph.add_edge(START, "reset")
    graph.add_edge("reset", END)
    compiled = graph.compile()

    result = compiled.invoke({"items": ["a", "b", "c"]})
    assert result["items"] == ["reset"]


# ---- 8. Checkpointing ----

class CpState(TypedDict):
    count: int


def increment_count(state: CpState) -> dict:
    return {"count": state["count"] + 1}


def test_checkpoint_basic():
    """Test basic checkpointing with InMemorySaver."""
    checkpointer = InMemorySaver()

    graph = StateGraph(CpState)
    graph.add_node("inc", increment_count)
    graph.add_edge(START, "inc")
    graph.add_edge("inc", END)
    compiled = graph.compile(checkpointer=checkpointer)

    config = {"configurable": {"thread_id": "test-thread"}}

    # First invocation
    result1 = compiled.invoke({"count": 0}, config)
    assert result1["count"] == 1

    # Second invocation on same thread - state persists
    result2 = compiled.invoke({"count": result1["count"]}, config)
    assert result2["count"] == 2


def test_get_state():
    """Test get_state."""
    checkpointer = InMemorySaver()

    graph = StateGraph(CpState)
    graph.add_node("inc", increment_count)
    graph.add_edge(START, "inc")
    graph.add_edge("inc", END)
    compiled = graph.compile(checkpointer=checkpointer)

    config = {"configurable": {"thread_id": "state-thread"}}

    compiled.invoke({"count": 0}, config)
    state = compiled.get_state(config)
    assert state.values["count"] == 1


# ---- 9. Method chaining ----

def test_method_chaining():
    """Test that builder methods return self for chaining."""
    graph = (
        StateGraph(SimpleState)
        .add_node("add", add_one)
        .add_edge(START, "add")
        .add_edge("add", END)
    )
    compiled = graph.compile()
    result = compiled.invoke({"value": 1})
    assert result == {"value": 2}


# ---- 10. add_sequence ----

def test_add_sequence():
    """Test add_sequence method."""
    graph = StateGraph(SimpleState)
    graph.add_sequence([add_one, multiply_two, add_one])
    graph.add_edge(START, "add_one")
    graph.add_edge("add_one", END)

    compiled = graph.compile()
    result = compiled.invoke({"value": 1})
    # add_one -> multiply_two -> add_one
    # 1 -> 2 -> 4 -> 5
    assert result["value"] == 5


# ---- 11. Validation errors ----

def test_duplicate_node():
    """Test that duplicate node names raise ValueError."""
    graph = StateGraph(SimpleState)
    graph.add_node("node", add_one)
    with pytest.raises(ValueError, match="already present"):
        graph.add_node("node", add_one)


def test_reserved_node_name():
    """Test that reserved names raise ValueError."""
    graph = StateGraph(SimpleState)
    with pytest.raises(ValueError, match="reserved"):
        graph.add_node(END, add_one)
    with pytest.raises(ValueError, match="reserved"):
        graph.add_node(START, add_one)


def test_no_entrypoint():
    """Test that graph without entrypoint fails validation."""
    graph = StateGraph(SimpleState)
    graph.add_node("node", add_one)
    with pytest.raises(ValueError, match="entrypoint"):
        graph.compile()


# ---- 12. Async execution ----

async def async_add(state: SimpleState) -> dict:
    return {"value": state["value"] + 10}


def test_async_invoke():
    """Test async graph execution."""
    graph = StateGraph(SimpleState)
    graph.add_node("add", async_add)
    graph.add_edge(START, "add")
    graph.add_edge("add", END)
    compiled = graph.compile()

    result = asyncio.run(compiled.ainvoke({"value": 5}))
    assert result == {"value": 15}


def test_async_stream():
    """Test async streaming."""
    graph = StateGraph(SimpleState)
    graph.add_node("add", async_add)
    graph.add_edge(START, "add")
    graph.add_edge("add", END)
    compiled = graph.compile()

    async def run():
        results = []
        async for event in compiled.astream({"value": 5}, stream_mode="updates"):
            results.append(event)
        return results

    results = asyncio.run(run())
    assert len(results) >= 1


# ---- 13. Root schema (non-TypedDict) ----

def test_root_schema():
    """Test graph with a simple type as root schema."""
    def inc(x: int) -> int:
        return x + 1

    graph = StateGraph(int)
    graph.add_node("inc", inc)
    graph.add_edge(START, "inc")
    graph.add_edge("inc", END)
    compiled = graph.compile()
    result = compiled.invoke(5)
    assert result == 6


# ---- 14. Debug stream mode ----

def test_stream_debug():
    """Test debug stream mode yields task/step info."""
    graph = StateGraph(SimpleState)
    graph.add_node("add", add_one)
    graph.add_edge(START, "add")
    graph.add_edge("add", END)
    compiled = graph.compile()

    events = list(compiled.stream({"value": 1}, stream_mode="debug"))
    assert len(events) >= 2
    task_events = [e for e in events if e["type"] == "task"]
    result_events = [e for e in events if e["type"] == "task_result"]
    step_events = [e for e in events if e["type"] == "step_end"]
    assert len(task_events) == 1
    assert task_events[0]["node"] == "add"
    assert len(result_events) == 1
    assert result_events[0]["result"] == {"value": 2}
    assert len(step_events) == 1


# ---- 15. RetryPolicy ----

call_count = 0

def flaky_node(state: SimpleState) -> dict:
    global call_count
    call_count += 1
    if call_count < 3:
        raise ValueError("flaky!")
    return {"value": state["value"] + 10}


def test_retry_policy():
    """Test RetryPolicy with exponential backoff."""
    global call_count
    call_count = 0

    graph = StateGraph(SimpleState)
    graph.add_node("flaky", flaky_node, retry_policy=RetryPolicy(
        max_attempts=3, initial_interval=0.01, backoff_factor=1.0, jitter=False,
    ))
    graph.add_edge(START, "flaky")
    graph.add_edge("flaky", END)
    compiled = graph.compile()

    result = compiled.invoke({"value": 5})
    assert result == {"value": 15}
    assert call_count == 3


def test_retry_policy_exhausted():
    """Test RetryPolicy raises after max attempts."""
    def always_fail(state: SimpleState) -> dict:
        raise ValueError("nope")

    graph = StateGraph(SimpleState)
    graph.add_node("always_fail", always_fail,
                    retry_policy=RetryPolicy(max_attempts=2, initial_interval=0.01))
    graph.add_edge(START, "always_fail")
    graph.add_edge("always_fail", END)
    compiled = graph.compile()

    with pytest.raises(ValueError, match="nope"):
        compiled.invoke({"value": 0})


# ---- 16. add_messages ----

def test_add_messages():
    """Test add_messages reducer for message management."""
    from zerograph import add_messages, RemoveMessage

    existing = [
        {"id": "1", "text": "hello"},
        {"id": "2", "text": "world"},
    ]
    new = [
        {"id": "2", "text": "updated"},
        {"id": "3", "text": "new"},
    ]
    result = add_messages(existing, new)
    assert len(result) == 3
    assert result[1]["text"] == "updated"
    assert result[2]["text"] == "new"


def test_add_messages_remove():
    """Test add_messages with RemoveMessage."""
    from zerograph import add_messages, RemoveMessage

    existing = [
        {"id": "1", "text": "hello"},
        {"id": "2", "text": "world"},
    ]
    result = add_messages(existing, [RemoveMessage("1")])
    assert len(result) == 1
    assert result[0]["text"] == "world"


def test_add_messages_in_graph():
    """Test add_messages as a reducer in a graph."""
    from typing import Annotated
    from zerograph import add_messages

    class MsgState(TypedDict):
        messages: Annotated[list, add_messages]

    def add_msg(state: MsgState) -> dict:
        return {"messages": [{"id": "2", "text": "new"}]}

    graph = StateGraph(MsgState)
    graph.add_node("add", add_msg)
    graph.add_edge(START, "add")
    graph.add_edge("add", END)
    compiled = graph.compile()

    result = compiled.invoke({"messages": [{"id": "1", "text": "hello"}]})
    assert len(result["messages"]) == 2
    assert result["messages"][0]["text"] == "hello"
    assert result["messages"][1]["text"] == "new"


# ---- 17. interrupt_after ----

def test_interrupt_after():
    """Test interrupt_after pauses execution after the specified node."""
    checkpointer = InMemorySaver()

    graph = StateGraph(SimpleState)
    graph.add_node("a", add_one)
    graph.add_node("b", multiply_two)
    graph.add_edge(START, "a")
    graph.add_edge("a", "b")
    graph.add_edge("b", END)
    compiled = graph.compile(
        checkpointer=checkpointer,
        interrupt_after=["a"],
    )

    config = {"configurable": {"thread_id": "ia-test"}}

    # First invoke stops after "a"
    result1 = compiled.invoke({"value": 3}, config)
    assert result1 == {"value": 4}  # 3+1=4, stopped before multiply

    # Resume - should run "b"
    result2 = compiled.invoke(Command(resume=None), config)
    assert result2 == {"value": 8}  # 4*2=8


# ---- 18. Command.goto ----

def test_command_goto():
    """Test Command.goto for routing control."""
    class GotoState(TypedDict):
        value: int
        steps: Annotated[list, operator.add]

    def node_a(state: GotoState) -> dict:
        if state["value"] < 3:
            return Command(update={"value": state["value"] + 1, "steps": ["a"]}, goto="b")
        return {"value": state["value"], "steps": ["a_final"]}

    def node_b(state: GotoState) -> dict:
        return {"value": state["value"] * 10, "steps": ["b"]}

    graph = StateGraph(GotoState)
    graph.add_node("a", node_a)
    graph.add_node("b", node_b)
    graph.add_edge(START, "a")
    graph.add_edge("b", END)
    compiled = graph.compile()

    result = compiled.invoke({"value": 0, "steps": []})
    # value goes 0->1 (goto b) -> 10
    assert result["value"] == 10
    assert "a" in result["steps"]


# ---- 19. Waiting edges (fan-in) ----

def test_waiting_edges():
    """Test waiting edges: target runs only after all sources complete."""
    class FanInState(TypedDict):
        results: Annotated[list, operator.add]

    def node_a(state: FanInState) -> dict:
        return {"results": ["a"]}

    def node_b(state: FanInState) -> dict:
        return {"results": ["b"]}

    def combine(state: FanInState) -> dict:
        return {"results": ["combined"]}

    graph = StateGraph(FanInState)
    graph.add_node("a", node_a)
    graph.add_node("b", node_b)
    graph.add_node("combine", combine)
    graph.add_edge(START, "a")
    graph.add_edge(START, "b")
    graph.add_edge(["a", "b"], "combine")
    graph.add_edge("combine", END)
    compiled = graph.compile()

    result = compiled.invoke({"results": []})
    assert "a" in result["results"]
    assert "b" in result["results"]
    assert "combined" in result["results"]


# ---- 20. TimeoutPolicy ----

def test_timeout_policy():
    """Test TimeoutPolicy is stored in node spec."""
    graph = StateGraph(SimpleState)
    graph.add_node("slow", add_one, timeout=5.0)
    graph.add_edge(START, "slow")
    graph.add_edge("slow", END)
    compiled = graph.compile()

    # Should still work normally
    result = compiled.invoke({"value": 1})
    assert result == {"value": 2}
    assert compiled.builder.nodes["slow"].timeout.run_timeout == 5.0
