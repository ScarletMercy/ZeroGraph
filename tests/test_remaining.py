"""Tests for remaining features: previous, state_history, stream combo, subgraph NS, update_state as_node."""

import asyncio
import pytest

from typing import Annotated, TypedDict

from zerograph import (
    StateGraph,
    InMemorySaver,
    Command,
    START,
    END,
    add_messages,
    entrypoint,
    task,
)
from zerograph.checkpoint.sqlite import SqliteSaver


class St(TypedDict):
    x: int
    y: str


def _inc(state):
    return {"x": state["x"] + 1}


def _double(state):
    return {"x": state["x"] * 2}


class TestPrevious:

    def test_previous_none_without_checkpointer(self):
        @entrypoint()
        def wf(inp, *, previous=None):
            return {"prev": previous, "val": inp["v"]}

        result = wf.invoke({"v": 1})
        assert result == {"prev": None, "val": 1}

    def test_previous_across_invocations(self):
        checkpointer = InMemorySaver()

        @entrypoint(checkpointer=checkpointer)
        def wf(inp, *, previous=None):
            return {"prev": previous, "val": inp["v"]}

        config = {"configurable": {"thread_id": "t1"}}

        r1 = wf.invoke({"v": 1}, config)
        assert r1 == {"prev": None, "val": 1}

        r2 = wf.invoke({"v": 2}, config)
        assert r2["prev"] == {"prev": None, "val": 1}
        assert r2["val"] == 2

    def test_previous_different_threads(self):
        checkpointer = InMemorySaver()

        @entrypoint(checkpointer=checkpointer)
        def wf(inp, *, previous=None):
            return {"prev": previous, "val": inp["v"]}

        wf.invoke({"v": "A"}, {"configurable": {"thread_id": "t1"}})
        wf.invoke({"v": "B"}, {"configurable": {"thread_id": "t2"}})

        r = wf.invoke({"v": "C"}, {"configurable": {"thread_id": "t1"}})
        # t1's previous should be the first result, not t2's
        assert r["prev"]["val"] == "A"

    def test_previous_async(self):
        checkpointer = InMemorySaver()

        @entrypoint(checkpointer=checkpointer)
        async def wf(inp, *, previous=None):
            return {"prev": previous, "val": inp["v"]}

        config = {"configurable": {"thread_id": "t1"}}
        r1 = asyncio.run(wf.ainvoke({"v": 10}, config))
        assert r1["val"] == 10

        r2 = asyncio.run(wf.ainvoke({"v": 20}, config))
        assert r2["prev"] == {"prev": None, "val": 10}


class TestStateHistory:

    def test_state_history_basic(self):
        checkpointer = InMemorySaver()
        g = StateGraph(St)
        g.add_node("inc", _inc)
        g.add_edge(START, "inc")
        g.add_edge("inc", END)
        compiled = g.compile(checkpointer=checkpointer)

        compiled.invoke({"x": 0, "y": "a"}, {"configurable": {"thread_id": "t1"}})
        compiled.invoke({"x": 0, "y": "a"}, {"configurable": {"thread_id": "t1"}})

        history = compiled.get_state_history({"configurable": {"thread_id": "t1"}})
        assert len(history) >= 2
        # Newest first
        assert history[0].values["x"] == 1

    def test_state_history_no_checkpointer(self):
        g = StateGraph(St)
        g.add_node("inc", _inc)
        g.add_edge(START, "inc")
        g.add_edge("inc", END)
        compiled = g.compile()
        history = compiled.get_state_history({"configurable": {"thread_id": "t1"}})
        assert history == []

    def test_state_history_with_sqlite(self):
        with SqliteSaver(":memory:") as saver:
            g = StateGraph(St)
            g.add_node("inc", _inc)
            g.add_edge(START, "inc")
            g.add_edge("inc", END)
            compiled = g.compile(checkpointer=saver)

            for i in range(5):
                compiled.invoke({"x": 0, "y": "a"}, {"configurable": {"thread_id": "t1"}})

            history = compiled.get_state_history(
                {"configurable": {"thread_id": "t1", "checkpoint_ns": ""}},
                limit=3,
            )
            assert len(history) == 3


class TestStreamModeCombo:

    def test_single_mode_backwards_compat(self):
        g = StateGraph(St)
        g.add_node("inc", _inc)
        g.add_edge(START, "inc")
        g.add_edge("inc", END)
        compiled = g.compile()

        events = list(compiled.stream({"x": 0, "y": "a"}, stream_mode="updates"))
        assert all(isinstance(e, dict) for e in events)

    def test_dual_mode_values_and_updates(self):
        g = StateGraph(St)
        g.add_node("inc", _inc)
        g.add_edge(START, "inc")
        g.add_edge("inc", END)
        compiled = g.compile()

        events = list(compiled.stream(
            {"x": 0, "y": "a"},
            stream_mode=["values", "updates"],
        ))
        # Events should be tuples (mode, data)
        modes_seen = set()
        for ev in events:
            assert isinstance(ev, tuple)
            assert len(ev) == 2
            modes_seen.add(ev[0])
        assert "values" in modes_seen
        assert "updates" in modes_seen

    def test_triple_mode(self):
        g = StateGraph(St)
        g.add_node("inc", _inc)
        g.add_edge(START, "inc")
        g.add_edge("inc", END)
        compiled = g.compile()

        events = list(compiled.stream(
            {"x": 0, "y": "a"},
            stream_mode=["values", "updates", "debug"],
        ))
        modes_seen = {ev[0] for ev in events}
        assert modes_seen == {"values", "updates", "debug"}


class TestSubgraphCheckpointNS:

    def test_subgraph_ns_isolation(self):
        checkpointer = InMemorySaver()

        class ParentSt(TypedDict):
            x: int

        class ChildSt(TypedDict):
            x: int

        def child_node(state):
            return {"x": state["x"] + 10}

        child = StateGraph(ChildSt)
        child.add_node("inner", child_node)
        child.add_edge(START, "inner")
        child.add_edge("inner", END)
        child_compiled = child.compile()

        parent = StateGraph(ParentSt)
        parent.add_node("sub", child_compiled)
        parent.add_edge(START, "sub")
        parent.add_edge("sub", END)
        parent_compiled = parent.compile(checkpointer=checkpointer)

        result = parent_compiled.invoke(
            {"x": 1},
            {"configurable": {"thread_id": "t1"}},
        )
        assert result["x"] == 11

        # Verify checkpoints don't conflict
        history = parent_compiled.get_state_history(
            {"configurable": {"thread_id": "t1"}}
        )
        assert len(history) >= 1


class TestUpdateStateAsNode:

    def test_update_state_sets_next_nodes(self):
        checkpointer = InMemorySaver()

        g = StateGraph(St)
        g.add_node("inc", _inc)
        g.add_node("double", _double)
        g.add_edge(START, "inc")
        g.add_edge("inc", "double")
        g.add_edge("double", END)
        compiled = g.compile(checkpointer=checkpointer)

        config = {"configurable": {"thread_id": "t1"}}
        compiled.invoke({"x": 1, "y": "a"}, config)

        # Update state as if it came from "inc" node
        compiled.update_state(config, {"x": 100}, as_node="inc")

        # After update with as_node="inc", next should be "double"
        state = compiled.get_state(config)
        assert state.values["x"] == 100
        assert "double" in state.next

    def test_update_state_without_as_node(self):
        checkpointer = InMemorySaver()

        g = StateGraph(St)
        g.add_node("inc", _inc)
        g.add_edge(START, "inc")
        g.add_edge("inc", END)
        compiled = g.compile(checkpointer=checkpointer)

        config = {"configurable": {"thread_id": "t1"}}
        compiled.invoke({"x": 1, "y": "a"}, config)
        compiled.update_state(config, {"x": 999})
        state = compiled.get_state(config)
        assert state.values["x"] == 999

    def test_update_state_as_node_with_conditional(self):
        checkpointer = InMemorySaver()

        g = StateGraph(St)
        g.add_node("inc", _inc)
        g.add_node("double", _double)
        g.add_edge(START, "inc")
        g.add_conditional_edges("inc", lambda s: "double" if s["x"] > 5 else END)
        g.add_edge("double", END)
        compiled = g.compile(checkpointer=checkpointer)

        config = {"configurable": {"thread_id": "t1"}}
        compiled.invoke({"x": 1, "y": "a"}, config)

        # Update as "inc" — conditional edge targets include "double"
        compiled.update_state(config, {"x": 100}, as_node="inc")
        state = compiled.get_state(config)
        assert "double" in state.next
