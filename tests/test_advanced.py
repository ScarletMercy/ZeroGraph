"""Tests for advanced ZeroGraph features."""

import asyncio
from typing import Annotated, TypedDict

import pytest

from zerograph import (
    START,
    END,
    StateGraph,
    CompiledStateGraph,
    InMemorySaver,
    AnyValue,
    InMemoryCache,
    CachePolicy,
    InMemoryStore,
    StoreItem,
    Send,
    Command,
    entrypoint,
    task,
)


# ---- AnyValue Channel ----

class TestAnyValue:
    def test_any_value_basic(self):
        ch = AnyValue(int, "test")
        assert not ch.is_available()
        ch.update([42])
        assert ch.is_available()
        assert ch.get() == 42

    def test_any_value_multiple_writes(self):
        ch = AnyValue(int, "test")
        ch.update([1, 2, 3])
        assert ch.get() == 3

    def test_any_value_in_graph(self):
        class State(TypedDict):
            x: Annotated[int, AnyValue]

        def node_a(state: State) -> dict:
            return {"x": state["x"] + 1}

        def node_b(state: State) -> dict:
            return {"x": state["x"] * 2}

        g = StateGraph(State)
        g.add_node("a", node_a)
        g.add_node("b", node_b)
        g.add_edge(START, "a")
        g.add_edge("a", "b")
        g.add_edge("b", END)
        result = g.compile().invoke({"x": 1})
        assert result == {"x": 4}


# ---- context_schema ----

class TestContextSchema:
    def test_context_schema_basic(self):
        class MyContext(TypedDict):
            user_id: str

        class State(TypedDict):
            count: int

        def increment(state: State, config) -> dict:
            ctx = config["configurable"]["__context__"]
            assert ctx["user_id"] == "alice"
            return {"count": state["count"] + 1}

        g = StateGraph(State, context_schema=MyContext)
        g.add_node("inc", increment)
        g.add_edge(START, "inc")
        g.add_edge("inc", END)
        result = g.compile(context={"user_id": "alice"}).invoke({"count": 0})
        assert result == {"count": 1}

    def test_context_schema_conflict(self):
        class MyContext(TypedDict):
            count: str  # conflicts with state

        class State(TypedDict):
            count: int

        with pytest.raises(ValueError, match="conflicts"):
            StateGraph(State, context_schema=MyContext)


# ---- "custom" stream mode ----

class TestCustomStreamMode:
    def test_custom_stream(self):
        class State(TypedDict):
            value: int

        def node(state: State, config) -> dict:
            writer = config["configurable"].get("__writer__")
            if writer:
                writer("step1")
                writer("step2")
            return {"value": state["value"] + 1}

        g = StateGraph(State)
        g.add_node("inc", node)
        g.add_edge(START, "inc")
        g.add_edge("inc", END)

        events = list(g.compile().stream({"value": 0}, stream_mode="custom"))
        assert len(events) == 2
        assert events[0] == {"node": "inc", "value": "step1"}
        assert events[1] == {"node": "inc", "value": "step2"}


# ---- "messages" stream mode ----

class TestMessagesStreamMode:
    def test_messages_stream_generator(self):
        class State(TypedDict):
            text: str

        def streaming_node(state: State):
            chunks = ["Hello", " ", "World"]
            for chunk in chunks:
                yield chunk
            return {"text": "Hello World"}

        g = StateGraph(State)
        g.add_node("stream", streaming_node)
        g.add_edge(START, "stream")
        g.add_edge("stream", END)

        events = list(g.compile().stream({"text": ""}, stream_mode="messages"))
        assert len(events) == 3
        assert events[0] == {"node": "stream", "chunk": "Hello"}
        assert events[1] == {"node": "stream", "chunk": " "}
        assert events[2] == {"node": "stream", "chunk": "World"}


# ---- batch execution ----

class TestBatch:
    def test_batch(self):
        class State(TypedDict):
            count: int

        def inc(state: State) -> dict:
            return {"count": state["count"] + 1}

        g = StateGraph(State)
        g.add_node("inc", inc)
        g.add_edge(START, "inc")
        g.add_edge("inc", END)
        compiled = g.compile()

        results = compiled.batch([{"count": 1}, {"count": 10}, {"count": 100}])
        assert results == [{"count": 2}, {"count": 11}, {"count": 101}]

    def test_abatch(self):
        class State(TypedDict):
            count: int

        async def ainc(state: State) -> dict:
            return {"count": state["count"] + 1}

        g = StateGraph(State)
        g.add_node("inc", ainc)
        g.add_edge(START, "inc")
        g.add_edge("inc", END)
        compiled = g.compile()

        results = asyncio.run(compiled.abatch([{"count": 1}, {"count": 10}]))
        assert results == [{"count": 2}, {"count": 11}]


# ---- Cache system ----

class TestCache:
    def test_cache_basic(self):
        call_count = 0

        class State(TypedDict):
            x: int

        def expensive(state: State) -> dict:
            nonlocal call_count
            call_count += 1
            return {"x": state["x"] * 2}

        cache = InMemoryCache()
        g = StateGraph(State)
        g.add_node("double", expensive, cache_policy=CachePolicy(ttl=60))
        g.add_edge(START, "double")
        g.add_edge("double", END)

        compiled = g.compile(cache=cache)
        r1 = compiled.invoke({"x": 5})
        assert r1 == {"x": 10}
        assert call_count == 1

        r2 = compiled.invoke({"x": 5})
        assert r2 == {"x": 10}
        assert call_count == 1  # cached, not called again

    def test_cache_ttl_expiry(self):
        call_count = 0

        class State(TypedDict):
            x: int

        def compute(state: State) -> dict:
            nonlocal call_count
            call_count += 1
            return {"x": state["x"] + 1}

        cache = InMemoryCache()
        g = StateGraph(State)
        g.add_node("inc", compute, cache_policy=CachePolicy(ttl=0.0))
        g.add_edge(START, "inc")
        g.add_edge("inc", END)

        compiled = g.compile(cache=cache)
        compiled.invoke({"x": 1})
        assert call_count == 1

        compiled.invoke({"x": 1})
        assert call_count == 2  # TTL expired, called again

    def test_cache_miss_different_input(self):
        call_count = 0

        class State(TypedDict):
            x: int

        def compute(state: State) -> dict:
            nonlocal call_count
            call_count += 1
            return {"x": state["x"] + 1}

        cache = InMemoryCache()
        g = StateGraph(State)
        g.add_node("inc", compute, cache_policy=CachePolicy(ttl=60))
        g.add_edge(START, "inc")
        g.add_edge("inc", END)

        compiled = g.compile(cache=cache)
        compiled.invoke({"x": 1})
        compiled.invoke({"x": 2})  # different input = cache miss
        assert call_count == 2


# ---- Store system ----

class TestStore:
    def test_store_basic(self):
        store = InMemoryStore()
        store.put("ns1", "key1", "value1")
        item = store.get("ns1", "key1")
        assert item is not None
        assert item.value == "value1"

    def test_store_search(self):
        store = InMemoryStore()
        store.put("ns1", "user_1", {"name": "Alice"})
        store.put("ns1", "user_2", {"name": "Bob"})
        store.put("ns1", "order_1", {"id": 1})

        results = store.search("ns1", prefix="user_")
        assert len(results) == 2

    def test_store_delete(self):
        store = InMemoryStore()
        store.put("ns1", "key1", "value1")
        store.delete("ns1", "key1")
        assert store.get("ns1", "key1") is None

    def test_store_in_graph(self):
        class State(TypedDict):
            count: int

        def read_store(state: State, config) -> dict:
            store = config["configurable"]["__store__"]
            store.put("results", "last_count", state["count"])
            return {"count": state["count"] + 1}

        store = InMemoryStore()
        g = StateGraph(State)
        g.add_node("inc", read_store)
        g.add_edge(START, "inc")
        g.add_edge("inc", END)

        compiled = g.compile(store=store)
        compiled.invoke({"count": 0})

        item = store.get("results", "last_count")
        assert item is not None
        assert item.value == 0


# ---- Graph visualization ----

class TestVisualization:
    def test_get_mermaid(self):
        class State(TypedDict):
            x: int

        def a(state): return {"x": 1}
        def b(state): return {"x": 2}

        g = StateGraph(State)
        g.add_node("a", a)
        g.add_node("b", b)
        g.add_edge(START, "a")
        g.add_edge("a", "b")
        g.add_edge("b", END)

        mermaid = g.get_graph()
        assert "flowchart TD" in mermaid
        assert '__start__(["START"])' in mermaid
        assert '__end__(["END"])' in mermaid
        assert 'a["a"]' in mermaid
        assert 'b["b"]' in mermaid
        assert "__start__ --> a" in mermaid
        assert "a --> b" in mermaid
        assert "b --> __end__" in mermaid

    def test_mermaid_conditional_edges(self):
        class State(TypedDict):
            x: int

        def route(state):
            return "b" if state["x"] > 0 else "c"

        g = StateGraph(State)
        g.add_node("a", lambda s: s)
        g.add_node("b", lambda s: s)
        g.add_node("c", lambda s: s)
        g.add_edge(START, "a")
        g.add_conditional_edges("a", route, {"b": "b", "c": "c"})
        g.add_edge("b", END)
        g.add_edge("c", END)

        mermaid = g.get_graph()
        assert 'a -->|"b"| b' in mermaid
        assert 'a -->|"c"| c' in mermaid


# ---- Subgraph ----

class TestSubgraph:
    def test_subgraph_basic(self):
        class SharedState(TypedDict):
            value: int

        def child_double(state: SharedState) -> dict:
            return {"value": state["value"] * 2}

        child = StateGraph(SharedState)
        child.add_node("double", child_double)
        child.add_edge(START, "double")
        child.add_edge("double", END)
        child_compiled = child.compile()

        def prepare(state: SharedState) -> dict:
            return {"value": state["value"] + 1}

        parent = StateGraph(SharedState)
        parent.add_node("prepare", prepare)
        parent.add_node("subgraph", child_compiled)
        parent.add_edge(START, "prepare")
        parent.add_edge("prepare", "subgraph")
        parent.add_edge("subgraph", END)

        result = parent.compile().invoke({"value": 3})
        assert result == {"value": 8}  # 3+1=4, then 4*2=8

    def test_subgraph_with_conditional(self):
        class SharedState(TypedDict):
            value: int

        def child_add(state: SharedState) -> dict:
            return {"value": state["value"] + 10}

        child = StateGraph(SharedState)
        child.add_node("add", child_add)
        child.add_edge(START, "add")
        child.add_edge("add", END)
        child_compiled = child.compile()

        def route(state: SharedState):
            if state["value"] > 5:
                return "sub"
            return END

        parent = StateGraph(SharedState)
        parent.add_node("sub", child_compiled)
        parent.add_conditional_edges(START, route, {"sub": "sub"})
        parent.add_edge("sub", END)

        result = parent.compile().invoke({"value": 10})
        assert result == {"value": 20}  # 10+10=20


# ---- Functional API ----

class TestFunctionalAPI:
    def test_task_basic(self):
        @task
        def add(a, b):
            return a + b

        future = add(1, 2)
        assert future.result() == 3

    def test_task_cached(self):
        call_count = 0

        @task
        def compute(x):
            nonlocal call_count
            call_count += 1
            return x * 2

        future = compute(5)
        assert future.result() == 10
        assert future.result() == 10  # cached in future
        assert call_count == 1

    def test_entrypoint_invoke(self):
        @task
        def add(a, b):
            return a + b

        @entrypoint()
        def workflow(inp):
            result = add(inp["x"], inp["y"])
            return result.result()

        assert workflow.invoke({"x": 3, "y": 4}) == 7

    def test_entrypoint_async(self):
        @task
        async def multiply(a, b):
            return a * b

        @entrypoint()
        async def workflow(inp):
            result = await multiply(inp["x"], inp["y"]).aresult()
            return result

        assert asyncio.run(workflow.ainvoke({"x": 3, "y": 4})) == 12

    def test_entrypoint_with_writer(self):
        events = []

        @entrypoint()
        def workflow(inp, *, writer=None):
            if writer:
                writer("hello")
            return inp["x"] * 2

        result = workflow.invoke({"x": 5})
        assert result == 10

    def test_entrypoint_stream(self):
        @task
        def double(x):
            return x * 2

        @entrypoint()
        def workflow(inp):
            r = double(inp["x"])
            return r.result()

        events = list(workflow.stream({"x": 3}, stream_mode="updates"))
        assert len(events) == 1
        assert events[0]["entrypoint"] == 6


# ---- Integration: multiple features together ----

class TestIntegration:
    def test_cache_with_subgraph(self):
        call_count = 0

        class SharedState(TypedDict):
            value: int

        def child_func(state: SharedState) -> dict:
            nonlocal call_count
            call_count += 1
            return {"value": state["value"] * 3}

        child = StateGraph(SharedState)
        child.add_node("triple", child_func)
        child.add_edge(START, "triple")
        child.add_edge("triple", END)

        parent = StateGraph(SharedState)
        parent.add_node("sub", child.compile(), cache_policy=CachePolicy(ttl=60))
        parent.add_edge(START, "sub")
        parent.add_edge("sub", END)

        cache = InMemoryCache()
        compiled = parent.compile(cache=cache)
        r1 = compiled.invoke({"value": 2})
        assert r1 == {"value": 6}
        assert call_count == 1

        r2 = compiled.invoke({"value": 2})
        assert r2 == {"value": 6}
        assert call_count == 1  # cached

    def test_store_with_batch(self):
        store = InMemoryStore()

        class State(TypedDict):
            x: int

        def save(state: State, config) -> dict:
            s = config["configurable"]["__store__"]
            s.put("history", f"run_{state['x']}", state["x"])
            return {"x": state["x"] + 1}

        g = StateGraph(State)
        g.add_node("inc", save)
        g.add_edge(START, "inc")
        g.add_edge("inc", END)
        compiled = g.compile(store=store)

        compiled.batch([{"x": 1}, {"x": 2}, {"x": 3}])
        assert store.get("history", "run_1").value == 1
        assert store.get("history", "run_2").value == 2
        assert store.get("history", "run_3").value == 3


# ---- Edge case tests for bug fixes ----

class TestEdgeCases:
    def test_send_with_config(self):
        """C6 fix: Send-triggered nodes should receive correct config."""
        class State(TypedDict):
            items: Annotated[list, lambda a, b: a + [b]]

        seen_task_ids = []

        def fan_out(state: State):
            return [Send("process", item) for item in ["a", "b"]]

        def process(item, config):
            tid = config["configurable"]["__task_id__"]
            seen_task_ids.append(tid)
            return {"items": item.upper()}

        g = StateGraph(State)
        g.add_node("process", process)
        g.add_conditional_edges(START, fan_out)
        g.add_edge("process", END)

        result = g.compile().invoke({"items": []})
        # Input [] is applied first, then Send results are appended
        assert result == {"items": [[], "A", "B"]}
        assert len(seen_task_ids) == 2
        assert seen_task_ids[0] != seen_task_ids[1]

    def test_messages_stream_with_config(self):
        """C1 fix: Generator nodes in messages mode should receive config."""
        class State(TypedDict):
            text: str

        def streaming_node(state, config):
            tid = config["configurable"]["__task_id__"]
            assert tid is not None
            yield f"task_id={tid}"
            return {"text": "done"}

        g = StateGraph(State)
        g.add_node("stream", streaming_node)
        g.add_edge(START, "stream")
        g.add_edge("stream", END)

        events = list(g.compile().stream({"text": ""}, stream_mode="messages"))
        assert len(events) == 1
        assert "task_id=" in events[0]["chunk"]

    def test_sync_generator_in_async_path(self):
        """I1 fix: Sync generators should work in astream with messages mode."""
        class State(TypedDict):
            text: str

        def sync_gen_node(state):
            yield "hello"
            yield "world"
            return {"text": "done"}

        g = StateGraph(State)
        g.add_node("gen", sync_gen_node)
        g.add_edge(START, "gen")
        g.add_edge("gen", END)

        events = asyncio.run(
            self._collect_astream(g.compile(), {"text": ""}, "messages")
        )
        assert len(events) == 2
        assert events[0] == {"node": "gen", "chunk": "hello"}
        assert events[1] == {"node": "gen", "chunk": "world"}

    @staticmethod
    async def _collect_astream(compiled, inp, mode):
        events = []
        async for ev in compiled.astream(inp, stream_mode=mode):
            events.append(ev)
        return events

    def test_cache_none_result(self):
        """C4: Nodes returning None should not be cached (documented behavior)."""
        call_count = 0

        class State(TypedDict):
            x: int

        def maybe_none(state: State):
            nonlocal call_count
            call_count += 1
            if state["x"] < 0:
                return None
            return {"x": state["x"] * 2}

        cache = InMemoryCache()
        g = StateGraph(State)
        g.add_node("double", maybe_none, cache_policy=CachePolicy(ttl=60))
        g.add_edge(START, "double")
        g.add_edge("double", END)
        compiled = g.compile(cache=cache)

        r1 = compiled.invoke({"x": -1})
        assert r1 == {"x": -1}  # None result means no update
        assert call_count == 1

        r2 = compiled.invoke({"x": -1})
        assert call_count == 2  # None not cached, called again

    def test_custom_stream_with_send(self):
        """I7 fix: Send-target nodes should support custom writer."""
        class State(TypedDict):
            items: Annotated[list, lambda a, b: a + [b]]

        def fan_out(state: State):
            return [Send("process", "x")]

        def process(item, config):
            writer = config["configurable"].get("__writer__")
            if writer:
                writer(f"processing {item}")
            return {"items": item.upper()}

        g = StateGraph(State)
        g.add_node("process", process)
        g.add_conditional_edges(START, fan_out)
        g.add_edge("process", END)

        events = list(g.compile().stream({"items": []}, stream_mode="custom"))
        assert len(events) == 1
        assert events[0] == {"node": "process", "value": "processing x"}

    def test_subgraph_output_keys_subset(self):
        """Subgraph with overlapping keys should only pass matching state."""
        class SharedState(TypedDict):
            x: int
            y: int

        def child_inc(state: SharedState) -> dict:
            return {"x": state["x"] + 1, "y": state["y"] + 1}

        child = StateGraph(SharedState)
        child.add_node("inc", child_inc)
        child.add_edge(START, "inc")
        child.add_edge("inc", END)

        parent = StateGraph(SharedState)
        parent.add_node("sub", child.compile())
        parent.add_edge(START, "sub")
        parent.add_edge("sub", END)

        result = parent.compile().invoke({"x": 10, "y": 20})
        assert result == {"x": 11, "y": 21}
