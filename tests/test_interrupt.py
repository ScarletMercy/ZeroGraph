"""Tests for ZeroGraph interrupt/resume functionality."""

import asyncio
import pytest
from typing import Annotated
from typing_extensions import TypedDict
import operator

from zerograph import (
    StateGraph,
    START,
    END,
    Command,
    InMemorySaver,
    interrupt,
)


class ValueState(TypedDict):
    value: int


class CountState(TypedDict):
    count: int


class XYState(TypedDict):
    x: int
    y: int


def _run(coro):
    return asyncio.run(coro)


# ---- 1. interrupt() inside node basic ----


def test_interrupt_in_node_basic():
    class ApproveState(TypedDict):
        approved: bool

    def human_review(state):
        answer = interrupt("confirm:")
        return {"approved": answer}

    graph = StateGraph(ApproveState)
    graph.add_node("review", human_review)
    graph.add_edge(START, "review")
    graph.add_edge("review", END)

    checkpointer = InMemorySaver()
    app = graph.compile(checkpointer=checkpointer)
    config = {"configurable": {"thread_id": "int-basic"}}

    result1 = app.invoke({"approved": False}, config)
    snap = app.get_state(config)
    assert snap.next == ("review",)

    result2 = app.invoke(Command(resume=True), config)
    assert result2["approved"] is True


# ---- 2. interrupt_before ----


def test_interrupt_before_nodes():
    def node_a(state):
        return {"value": state["value"] + 1}

    def node_b(state):
        return {"value": state["value"] * 2}

    graph = StateGraph(ValueState)
    graph.add_node("a", node_a)
    graph.add_node("b", node_b)
    graph.add_edge(START, "a")
    graph.add_edge("a", "b")
    graph.add_edge("b", END)

    checkpointer = InMemorySaver()
    app = graph.compile(
        checkpointer=checkpointer,
        interrupt_before=["b"],
    )
    config = {"configurable": {"thread_id": "int-before"}}

    result1 = app.invoke({"value": 3}, config)
    assert result1["value"] == 4

    snap = app.get_state(config)
    assert snap.next == ("b",)

    result2 = app.invoke(Command(resume=None), config)
    assert result2["value"] == 8


# ---- 3. interrupt_after ----


def test_interrupt_after_nodes():
    def node_a(state):
        return {"value": state["value"] + 1}

    def node_b(state):
        return {"value": state["value"] * 2}

    graph = StateGraph(ValueState)
    graph.add_node("a", node_a)
    graph.add_node("b", node_b)
    graph.add_edge(START, "a")
    graph.add_edge("a", "b")
    graph.add_edge("b", END)

    checkpointer = InMemorySaver()
    app = graph.compile(
        checkpointer=checkpointer,
        interrupt_after=["a"],
    )
    config = {"configurable": {"thread_id": "int-after"}}

    result1 = app.invoke({"value": 3}, config)
    assert result1["value"] == 4

    result2 = app.invoke(Command(resume=None), config)
    assert result2["value"] == 8


# ---- 4. multistep interrupt/resume ----


def test_multistep_interrupt_resume():
    def step_a(state):
        return {"value": state["value"] + 1}

    def step_b(state):
        answer = interrupt("check value")
        return {"value": state["value"] * 10 + answer}

    def step_c(state):
        return {"value": state["value"] + 100}

    graph = StateGraph(ValueState)
    graph.add_node("a", step_a)
    graph.add_node("b", step_b)
    graph.add_node("c", step_c)
    graph.add_edge(START, "a")
    graph.add_edge("a", "b")
    graph.add_edge("b", "c")
    graph.add_edge("c", END)

    checkpointer = InMemorySaver()
    app = graph.compile(checkpointer=checkpointer)
    config = {"configurable": {"thread_id": "int-multi"}}

    result1 = app.invoke({"value": 5}, config)
    assert result1["value"] == 6

    result2 = app.invoke(Command(resume=7), config)
    assert result2["value"] == 167


# ---- 5. multiple interrupt calls in one node ----


def test_interrupt_in_two_node_chain():
    def ask_first(state):
        val = interrupt("question")
        return {"value": val}

    def double(state):
        return {"value": state["value"] * 2}

    graph = StateGraph(ValueState)
    graph.add_node("ask", ask_first)
    graph.add_node("double", double)
    graph.add_edge(START, "ask")
    graph.add_edge("ask", "double")
    graph.add_edge("double", END)

    checkpointer = InMemorySaver()
    app = graph.compile(checkpointer=checkpointer)
    config = {"configurable": {"thread_id": "int-chain"}}

    app.invoke({"value": 0}, config)
    result = app.invoke(Command(resume=5), config)
    assert result["value"] == 10


# ---- 6. get_state at interrupt point ----


def test_get_state_at_interrupt():
    def node_a(state):
        return {"value": state["value"] + 1}

    def node_b(state):
        interrupt("pause")
        return {"value": state["value"] * 2}

    graph = StateGraph(ValueState)
    graph.add_node("a", node_a)
    graph.add_node("b", node_b)
    graph.add_edge(START, "a")
    graph.add_edge("a", "b")
    graph.add_edge("b", END)

    checkpointer = InMemorySaver()
    app = graph.compile(checkpointer=checkpointer)
    config = {"configurable": {"thread_id": "int-getstate"}}

    app.invoke({"value": 3}, config)
    snap = app.get_state(config)

    assert snap.values is not None
    assert snap.values.get("value") == 4
    assert snap.next == ("b",)
    assert len(snap.interrupts) > 0


# ---- 7. interrupt without checkpointer raises ----


def test_interrupt_without_checkpointer():
    def node_with_interrupt(state):
        interrupt("no checkpointer")
        return {"value": 0}

    graph = StateGraph(ValueState)
    graph.add_node("n", node_with_interrupt)
    graph.add_edge(START, "n")
    graph.add_edge("n", END)

    app = graph.compile()
    result = app.invoke({"value": 1})
    assert result["value"] == 1


# ---- 8. interrupt with stream ----


def test_interrupt_with_stream():
    def node_a(state):
        return {"value": state["value"] + 1}

    def node_b(state):
        interrupt("wait")
        return {"value": state["value"] * 10}

    graph = StateGraph(ValueState)
    graph.add_node("a", node_a)
    graph.add_node("b", node_b)
    graph.add_edge(START, "a")
    graph.add_edge("a", "b")
    graph.add_edge("b", END)

    checkpointer = InMemorySaver()
    app = graph.compile(checkpointer=checkpointer)
    config = {"configurable": {"thread_id": "int-stream"}}

    events1 = list(app.stream({"value": 1}, config, stream_mode="updates"))
    assert len(events1) >= 1
    assert any("a" in e for e in events1)

    events2 = list(app.stream(Command(resume=True), config, stream_mode="updates"))
    assert any("b" in e for e in events2)


# ---- 9. interrupt state history ----


def test_interrupt_state_history():
    def node_a(state):
        return {"value": state["value"] + 1}

    def node_b(state):
        interrupt("pause")
        return {"value": state["value"] * 2}

    graph = StateGraph(ValueState)
    graph.add_node("a", node_a)
    graph.add_node("b", node_b)
    graph.add_edge(START, "a")
    graph.add_edge("a", "b")
    graph.add_edge("b", END)

    checkpointer = InMemorySaver()
    app = graph.compile(checkpointer=checkpointer)
    config = {"configurable": {"thread_id": "int-history"}}

    app.invoke({"value": 5}, config)

    history = app.get_state_history(config)
    assert len(history) >= 2


# ---- 10. async interrupt/resume ----


def test_async_interrupt_resume():
    async def _test():
        def node_a(state):
            return {"value": state["value"] + 1}

        def node_b(state):
            answer = interrupt("confirm")
            return {"value": state["value"] + answer}

        graph = StateGraph(ValueState)
        graph.add_node("a", node_a)
        graph.add_node("b", node_b)
        graph.add_edge(START, "a")
        graph.add_edge("a", "b")
        graph.add_edge("b", END)

        checkpointer = InMemorySaver()
        app = graph.compile(checkpointer=checkpointer)
        config = {"configurable": {"thread_id": "int-async"}}

        result1 = await app.ainvoke({"value": 10}, config)
        assert result1["value"] == 11

        result2 = await app.ainvoke(Command(resume=5), config)
        assert result2["value"] == 16

    _run(_test())


# ---- 11. interrupt_before on first node ----


def test_interrupt_before_first_node():
    def node_a(state):
        return {"value": state["value"] + 1}

    graph = StateGraph(ValueState)
    graph.add_node("a", node_a)
    graph.add_edge(START, "a")
    graph.add_edge("a", END)

    checkpointer = InMemorySaver()
    app = graph.compile(
        checkpointer=checkpointer,
        interrupt_before=["a"],
    )
    config = {"configurable": {"thread_id": "int-first"}}

    result1 = app.invoke({"value": 3}, config)
    assert result1["value"] == 3

    snap = app.get_state(config)
    assert snap.next == ("a",)

    result2 = app.invoke(Command(resume=None), config)
    assert result2["value"] == 4


# ---- 12. interrupt resume value returned ----


def test_interrupt_resume_value_returned():
    def ask_node(state):
        val = interrupt("give me a number")
        return {"value": val}

    graph = StateGraph(ValueState)
    graph.add_node("ask", ask_node)
    graph.add_edge(START, "ask")
    graph.add_edge("ask", END)

    checkpointer = InMemorySaver()
    app = graph.compile(checkpointer=checkpointer)
    config = {"configurable": {"thread_id": "int-val"}}

    app.invoke({"value": 0}, config)
    result = app.invoke(Command(resume=42), config)
    assert result["value"] == 42
