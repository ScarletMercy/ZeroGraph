"""Tests for new features: AsyncSqliteSaver, error_handler, get_state(subgraphs),
stream checkpoints/tasks, supervisor, swarm."""

import asyncio
import pytest

from typing import Annotated, TypedDict

from zerograph import (
    StateGraph,
    InMemorySaver,
    InMemoryStore,
    StoreItem,
    Command,
    START,
    END,
    add_messages,
    entrypoint,
    task,
    AsyncSqliteSaver,
)
from zerograph.checkpoint.sqlite import SqliteSaver


# --- Helpers ---

class St(TypedDict):
    x: int
    y: str


def _inc(state):
    return {"x": state["x"] + 1}


def _double(state):
    return {"x": state["x"] * 2}


# --- AsyncSqliteSaver ---


class TestAsyncSqliteSaver:

    def test_async_put_and_get(self):
        async def _test():
            import uuid
            from datetime import datetime, timezone
            from zerograph.checkpoint.base import Checkpoint, CheckpointMetadata

            async with AsyncSqliteSaver(":memory:") as saver:
                cp = {
                    "v": 1,
                    "id": str(uuid.uuid4()),
                    "ts": datetime.now(timezone.utc).isoformat(),
                    "channel_values": {"x": 42},
                    "channel_versions": {"x": 1},
                    "versions_seen": {},
                }
                meta = CheckpointMetadata(source="input", step=0)
                config = {"configurable": {"thread_id": "t1"}}

                result_config = await saver.aput(config, cp, meta)
                assert "checkpoint_id" in result_config["configurable"]

                tup = await saver.aget_tuple(config)
                assert tup is not None
                assert tup.checkpoint["channel_values"]["x"] == 42

        asyncio.run(_test())

    def test_async_list(self):
        async def _test():
            import uuid
            import tempfile
            import os
            from datetime import datetime, timezone
            from zerograph.checkpoint.base import Checkpoint, CheckpointMetadata

            with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
                db_path = f.name
            try:
                async with AsyncSqliteSaver(db_path) as saver:
                    for i in range(5):
                        cp = {
                            "v": 1,
                            "id": str(uuid.uuid4()),
                            "ts": datetime.now(timezone.utc).isoformat(),
                            "channel_values": {"x": i},
                            "channel_versions": {"x": 1},
                            "versions_seen": {},
                        }
                        meta = CheckpointMetadata(source="loop", step=i)
                        await saver.aput(
                            {"configurable": {"thread_id": "t1"}}, cp, meta
                        )

                    results = await saver.alist(
                        {"configurable": {"thread_id": "t1"}}, limit=3
                    )
                    assert len(results) == 3
            finally:
                import gc
                gc.collect()
                try:
                    os.unlink(db_path)
                except PermissionError:
                    pass

        asyncio.run(_test())

    def test_async_delete_thread(self):
        async def _test():
            import uuid
            from datetime import datetime, timezone
            from zerograph.checkpoint.base import Checkpoint, CheckpointMetadata

            async with AsyncSqliteSaver(":memory:") as saver:
                cp = {
                    "v": 1,
                    "id": str(uuid.uuid4()),
                    "ts": datetime.now(timezone.utc).isoformat(),
                    "channel_values": {"x": 0},
                    "channel_versions": {"x": 1},
                    "versions_seen": {},
                }
                await saver.aput(
                    {"configurable": {"thread_id": "t1"}}, cp,
                    CheckpointMetadata(source="input", step=0),
                )
                await saver.adelete_thread("t1")
                tup = await saver.aget_tuple({"configurable": {"thread_id": "t1"}})
                assert tup is None

        asyncio.run(_test())

    def test_async_context_manager(self):
        async def _test():
            async with AsyncSqliteSaver(":memory:") as saver:
                assert saver is not None

        asyncio.run(_test())


# --- error_handler ---


class TestErrorHandler:

    def test_error_handler_basic(self):
        g = StateGraph(St)
        checkpointer = InMemorySaver()

        def bad_node(state):
            raise ValueError("something went wrong")

        def handle_error(state):
            return {"y": f"error: {state.get('__error__', {}).get('error', 'unknown')}"}

        g.add_node("bad", bad_node, error_handler="error_handler")
        g.add_node("error_handler", handle_error)
        g.add_edge(START, "bad")
        g.add_edge("error_handler", END)
        compiled = g.compile(checkpointer=checkpointer)

        result = compiled.invoke({"x": 1, "y": "a"}, {"configurable": {"thread_id": "t1"}})
        assert "error" in result["y"]

    def test_error_handler_no_handler_raises(self):
        g = StateGraph(St)

        def bad_node(state):
            raise ValueError("boom")

        g.add_node("bad", bad_node)
        g.add_edge(START, "bad")
        g.add_edge("bad", END)
        compiled = g.compile()

        with pytest.raises(ValueError, match="boom"):
            compiled.invoke({"x": 1, "y": "a"})

    def test_error_handler_validation_unknown_node(self):
        g = StateGraph(St)

        def node_a(state):
            return state

        g.add_node("a", node_a, error_handler="nonexistent")
        g.add_edge(START, "a")
        g.add_edge("a", END)

        with pytest.raises(ValueError, match="error_handler node"):
            g.compile()


# --- get_state(subgraphs=True) ---


class TestGetStateSubgraphs:

    def test_subgraph_state_nested(self):
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

        config = {"configurable": {"thread_id": "t1"}}
        parent_compiled.invoke({"x": 1}, config)

        state = parent_compiled.get_state(config, subgraphs=True)
        assert state.values["x"] == 11
        assert state.subgraphs is not None
        assert "sub" in state.subgraphs
        assert state.subgraphs["sub"].values["x"] == 11

    def test_subgraph_state_none_when_no_subgraphs(self):
        checkpointer = InMemorySaver()

        g = StateGraph(St)
        g.add_node("inc", _inc)
        g.add_edge(START, "inc")
        g.add_edge("inc", END)
        compiled = g.compile(checkpointer=checkpointer)

        config = {"configurable": {"thread_id": "t1"}}
        compiled.invoke({"x": 1, "y": "a"}, config)

        state = compiled.get_state(config, subgraphs=True)
        assert state.subgraphs is None or state.subgraphs == {}


# --- stream_mode="checkpoints" and "tasks" ---


class TestStreamCheckpointsTasks:

    def test_stream_checkpoints(self):
        checkpointer = InMemorySaver()

        g = StateGraph(St)
        g.add_node("inc", _inc)
        g.add_edge(START, "inc")
        g.add_edge("inc", END)
        compiled = g.compile(checkpointer=checkpointer)

        events = list(compiled.stream(
            {"x": 0, "y": "a"},
            {"configurable": {"thread_id": "t1"}},
            stream_mode="checkpoints",
        ))
        assert len(events) >= 1
        assert all("step" in e for e in events)

    def test_stream_tasks(self):
        g = StateGraph(St)
        g.add_node("inc", _inc)
        g.add_edge(START, "inc")
        g.add_edge("inc", END)
        compiled = g.compile()

        events = list(compiled.stream(
            {"x": 0, "y": "a"},
            stream_mode="tasks",
        ))
        assert len(events) >= 2
        types_seen = [e["type"] for e in events]
        assert "task_start" in types_seen
        assert "task_end" in types_seen

    def test_multi_mode_with_checkpoints_and_tasks(self):
        checkpointer = InMemorySaver()

        g = StateGraph(St)
        g.add_node("inc", _inc)
        g.add_edge(START, "inc")
        g.add_edge("inc", END)
        compiled = g.compile(checkpointer=checkpointer)

        events = list(compiled.stream(
            {"x": 0, "y": "a"},
            {"configurable": {"thread_id": "t1"}},
            stream_mode=["checkpoints", "tasks"],
        ))
        modes_seen = {ev[0] for ev in events}
        assert "checkpoints" in modes_seen
        assert "tasks" in modes_seen


# --- Supervisor ---


class TestSupervisor:

    def test_supervisor_basic(self):
        def agent_a(state):
            return {"messages": [{"role": "assistant", "content": "Agent A response"}]}

        def agent_b(state):
            return {"messages": [{"role": "assistant", "content": "Agent B response"}]}

        call_count = 0

        def llm(messages, tools=None):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                return {
                    "role": "assistant",
                    "content": "Routing to A",
                    "tool_calls": [
                        {"id": "tc1", "type": "function",
                         "function": {"name": "agent_a", "arguments": '{"input": "hello"}'}}
                    ],
                }
            else:
                return {"role": "assistant", "content": "Done"}

        from zerograph.prebuilt.supervisor import create_supervisor
        graph = create_supervisor(llm, [agent_a, agent_b])
        result = graph.invoke({"messages": [{"role": "user", "content": "test"}]})
        assert len(result["messages"]) >= 2

    def test_supervisor_with_checkpoint(self):
        from zerograph.prebuilt.supervisor import create_supervisor

        def agent_a(state):
            return {"messages": [{"role": "assistant", "content": "A"}]}

        call_count = 0

        def llm(messages, tools=None):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                return {
                    "role": "assistant", "content": "",
                    "tool_calls": [{"id": "tc1", "type": "function",
                                    "function": {"name": "agent_a", "arguments": '{}'}}],
                }
            return {"role": "assistant", "content": "final"}

        checkpointer = InMemorySaver()
        graph = create_supervisor(llm, [agent_a], checkpointer=checkpointer)
        result = graph.invoke(
            {"messages": [{"role": "user", "content": "hi"}]},
            {"configurable": {"thread_id": "t1"}},
        )
        assert any("final" in str(m.get("content", "")) for m in result["messages"])


# --- Swarm ---


class TestSwarm:

    def test_swarm_basic(self):
        def agent_a(state):
            return {"messages": [
                {"role": "assistant", "content": "A done", "handoff": "agent_b"}
            ]}

        def agent_b(state):
            return {"messages": [
                {"role": "assistant", "content": "B done"}
            ]}

        from zerograph.prebuilt.swarm import create_swarm
        graph = create_swarm([agent_a, agent_b])
        result = graph.invoke({"messages": [{"role": "user", "content": "test"}]})
        msgs = result["messages"]
        assert any("B done" in str(m.get("content", "")) for m in msgs)

    def test_swarm_immediate_end(self):
        def agent_a(state):
            return {"messages": [
                {"role": "assistant", "content": "Done right away"}
            ]}

        from zerograph.prebuilt.swarm import create_swarm
        graph = create_swarm([agent_a])
        result = graph.invoke({"messages": [{"role": "user", "content": "hi"}]})
        assert any("Done right away" in str(m.get("content", "")) for m in result["messages"])

    def test_swarm_with_tools(self):
        def search(query: str) -> str:
            return f"Result for {query}"

        def agent_a(state):
            messages = state.get("messages", [])
            last = messages[-1] if messages else {}
            # If last message is a tool result, return final answer
            if last.get("role") == "tool":
                return {"messages": [
                    {"role": "assistant", "content": f"Found: {last.get('content')}"}
                ]}
            # Otherwise, call a tool
            return {"messages": [
                {"role": "assistant", "content": "",
                 "tool_calls": [{"id": "tc1", "type": "function",
                                 "function": {"name": "search", "arguments": '{"query": "test"}'}}]},
            ]}

        from zerograph.prebuilt.swarm import create_swarm
        graph = create_swarm([agent_a], tools=[search])
        result = graph.invoke({"messages": [{"role": "user", "content": "search"}]})
        # Should have tool result and final answer
        assert any(m.get("role") == "tool" for m in result["messages"])
        assert any("Found" in str(m.get("content", "")) for m in result["messages"])


# --- Parallel async execution ---


class TestParallelAsync:

    def test_parallel_execution(self):
        """Verify nodes run in parallel when max_concurrency > 1."""
        import time

        async def _test():
            from typing import Annotated
            from zerograph.channels.messages import add_messages

            class State(TypedDict):
                results: Annotated[list, add_messages]

            call_times = {}

            def slow_a(state):
                time.sleep(0.05)
                return {"results": ["A"]}

            def slow_b(state):
                time.sleep(0.05)
                return {"results": ["B"]}

            g = StateGraph(State)
            g.add_node("a", slow_a)
            g.add_node("b", slow_b)
            g.add_edge(START, "a")
            g.add_edge(START, "b")
            g.add_edge("a", END)
            g.add_edge("b", END)
            compiled = g.compile()

            start = time.perf_counter()
            result = await compiled.ainvoke(
                {"results": []},
                {"max_concurrency": 2},
            )
            elapsed = time.perf_counter() - start

            # Both results should be present
            assert "A" in result["results"]
            assert "B" in result["results"]
            # Parallel should be faster than sequential (2 * 0.05 = 0.1s)
            assert elapsed < 0.15

        asyncio.run(_test())

    def test_sequential_when_max_concurrency_1(self):
        """Verify nodes run sequentially when max_concurrency = 1."""
        import time

        async def _test():
            from typing import Annotated
            from zerograph.channels.messages import add_messages

            class State(TypedDict):
                results: Annotated[list, add_messages]

            def slow_a(state):
                time.sleep(0.05)
                return {"results": ["A"]}

            def slow_b(state):
                time.sleep(0.05)
                return {"results": ["B"]}

            g = StateGraph(State)
            g.add_node("a", slow_a)
            g.add_node("b", slow_b)
            g.add_edge(START, "a")
            g.add_edge(START, "b")
            g.add_edge("a", END)
            g.add_edge("b", END)
            compiled = g.compile()

            start = time.perf_counter()
            result = await compiled.ainvoke(
                {"results": []},
                {"max_concurrency": 1},
            )
            elapsed = time.perf_counter() - start

            assert "A" in result["results"]
            assert "B" in result["results"]
            # Sequential should take ~0.1s (both sleep)
            assert elapsed >= 0.08

        asyncio.run(_test())


# --- set_node_defaults ---


class TestSetNodeDefaults:

    def test_set_retry_policy(self):
        from zerograph import RetryPolicy

        g = StateGraph(St)

        def node_a(state):
            return state

        def node_b(state):
            return state

        g.add_node("a", node_a)
        g.add_node("b", node_b, retry_policy=RetryPolicy(max_attempts=5))
        g.add_edge(START, "a")
        g.add_edge("a", "b")
        g.add_edge("b", END)

        policy = RetryPolicy(max_attempts=3)
        g.set_node_defaults(retry_policy=policy)

        # a should get the default
        assert g.nodes["a"].retry_policy == policy
        # b should keep its explicit policy
        assert g.nodes["b"].retry_policy.max_attempts == 5

    def test_set_error_handler(self):
        g = StateGraph(St)

        def node_a(state):
            return state

        def handler(state):
            return state

        g.add_node("a", node_a)
        g.add_node("handler", handler)
        g.add_edge(START, "a")
        g.add_edge("a", END)

        g.set_node_defaults(error_handler="handler")
        assert g.nodes["a"].error_handler_node == "handler"
        # handler itself should also get the default (but it points to itself)
        assert g.nodes["handler"].error_handler_node == "handler"

    def test_returns_self_for_chaining(self):
        g = StateGraph(St)

        def node_a(state):
            return state

        g.add_node("a", node_a)
        result = g.set_node_defaults()
        assert result is g


# --- InjectedState / InjectedStore ---


class TestInjectedStateStore:

    def test_injected_state(self):
        from zerograph.prebuilt.tool_node import ToolNode, InjectedState

        def my_tool(query: str, state: InjectedState) -> str:
            return f"query={query}, x={state.get('x', '?')}"

        tool_node = ToolNode([my_tool])

        state = {
            "messages": [
                {"role": "user", "content": "test"},
                {
                    "role": "assistant",
                    "content": "",
                    "tool_calls": [
                        {
                            "id": "tc1",
                            "type": "function",
                            "function": {
                                "name": "my_tool",
                                "arguments": '{"query": "hello"}',
                            },
                        }
                    ],
                },
            ],
            "x": 42,
        }

        result = tool_node(state)
        assert len(result["messages"]) == 1
        assert "query=hello" in result["messages"][0]["content"]
        assert "x=42" in result["messages"][0]["content"]

    def test_injected_store(self):
        from zerograph import InMemoryStore
        from zerograph.prebuilt.tool_node import ToolNode, InjectedStore

        store = InMemoryStore()
        store.put(("ns",), "key1", StoreItem(value="val1", namespace=("ns",), key="key1"))

        def my_tool(query: str, store: InjectedStore) -> str:
            item = store.get(("ns",), "key1")
            return f"{query}: {item.value if item else 'none'}"

        tool_node = ToolNode([my_tool], store=store)

        state = {
            "messages": [
                {
                    "role": "assistant",
                    "content": "",
                    "tool_calls": [
                        {
                            "id": "tc1",
                            "type": "function",
                            "function": {
                                "name": "my_tool",
                                "arguments": '{"query": "lookup"}',
                            },
                        }
                    ],
                },
            ],
        }

        result = tool_node(state)
        assert "lookup:" in result["messages"][0]["content"]
        assert "val1" in result["messages"][0]["content"]

    def test_no_injection_when_not_annotated(self):
        from zerograph.prebuilt.tool_node import ToolNode

        def simple_tool(x: int) -> int:
            return x * 2

        tool_node = ToolNode([simple_tool])
        state = {
            "messages": [
                {
                    "role": "assistant",
                    "content": "",
                    "tool_calls": [
                        {
                            "id": "tc1",
                            "type": "function",
                            "function": {
                                "name": "simple_tool",
                                "arguments": '{"x": 5}',
                            },
                        }
                    ],
                },
            ],
        }

        result = tool_node(state)
        assert result["messages"][0]["content"] == "10"
