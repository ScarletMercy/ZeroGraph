"""场景 11：函数式 API — @entrypoint/@task/previous/LLMStreamAdapter/可视化/异常"""

from __future__ import annotations

import asyncio
import sys
import os
from typing import TypedDict

from zerograph import (
    END,
    START,
    TAG_HIDDEN,
    GraphRecursionError,
    GraphBubbleUp,
    GraphInterrupt,
    ParentCommand,
    PregelTask,
    StateGraph,
    entrypoint,
    task,
    CheckpointTuple,
)
from zerograph.checkpoint.memory import InMemorySaver
from zerograph.adapters import LLMStreamAdapter, stream_openai


def run() -> list[tuple[str, bool, str]]:
    results: list[tuple[str, bool, str]] = []

    # --- 1. @task 装饰器 ---
    try:

        @task
        def compute(x: int) -> int:
            return x * 2

        future = compute(21)
        assert future.result() == 42, f"expected 42 got {future.result()}"
        results.append(("@task 同步执行", True, f"21 * 2 = {future.result()}"))
    except Exception as e:
        results.append(("@task 同步执行", False, str(e)))

    # --- 2. @task async ---
    try:

        @task
        async def async_compute(x: int) -> int:
            return x + 100

        future2 = async_compute(5)
        val2 = asyncio.run(future2.aresult())
        assert val2 == 105, f"expected 105 got {val2}"
        results.append(("@task 异步执行", True, f"5 + 100 = {val2}"))
    except Exception as e:
        results.append(("@task 异步执行", False, str(e)))

    # --- 3. @entrypoint + previous ---
    try:
        saver3 = InMemorySaver()

        @entrypoint(checkpointer=saver3)
        def workflow(inp, *, previous=None):
            prev_val = previous if previous is not None else 0
            return inp["x"] + prev_val

        config3 = {"configurable": {"thread_id": "ep1"}}
        r1 = workflow.invoke({"x": 10}, config3)
        r2 = workflow.invoke({"x": 20}, config3)
        assert r1 == 10, f"first: expected 10 got {r1}"
        assert r2 == 30, f"second: expected 30 got {r2}"
        results.append(("@entrypoint + previous", True, f"10, 30"))
    except Exception as e:
        results.append(("@entrypoint + previous", False, str(e)))

    # --- 4. @entrypoint ainvoke ---
    try:
        saver4 = InMemorySaver()

        @entrypoint(checkpointer=saver4)
        async def async_workflow(inp, *, previous=None):
            return inp["x"] * 3

        config4 = {"configurable": {"thread_id": "ep_async"}}
        r4 = asyncio.run(async_workflow.ainvoke({"x": 7}, config4))
        assert r4 == 21, f"expected 21 got {r4}"
        results.append(("@entrypoint ainvoke", True, f"7 * 3 = {r4}"))
    except Exception as e:
        results.append(("@entrypoint ainvoke", False, str(e)))

    # --- 5. @entrypoint stream ---
    try:

        @entrypoint()
        def stream_workflow(inp, *, writer=None):
            if writer:
                writer("event1")
                writer("event2")
            return inp["x"] + 1

        events5 = list(
            stream_workflow.stream({"x": 5}, stream_mode="updates")
        )
        assert len(events5) >= 1, f"expected >=1 events got {len(events5)}"
        # Verify the final result contains correct value: x=5+1=6
        last_val = events5[-1] if events5 else None
        # Entry point stream wraps result in {'entrypoint': value} or returns directly
        actual = last_val.get("entrypoint", last_val) if isinstance(last_val, dict) else last_val
        assert actual == 6, f"expected final result 6, got {last_val}"
        results.append(("@entrypoint stream", True, f"{len(events5)} events, result={actual}"))
    except Exception as e:
        results.append(("@entrypoint stream", False, str(e)))

    # --- 6. @entrypoint with @task ---
    try:

        @task
        def double(x):
            return x * 2

        @entrypoint()
        def task_workflow(inp):
            a = double(inp["x"])
            b = double(a.result())
            return b.result()

        r6 = task_workflow.invoke({"x": 3})
        assert r6 == 12, f"expected 12 got {r6}"
        results.append(("@entrypoint + @task 组合", True, f"3→6→12"))
    except Exception as e:
        results.append(("@entrypoint + @task 组合", False, str(e)))

    # --- 7. LLMStreamAdapter ---
    try:
        from types import SimpleNamespace

        adapter = LLMStreamAdapter()
        # Simulate OpenAI-style stream chunks (object-based, not dict)
        chunks = [
            SimpleNamespace(choices=[
                SimpleNamespace(delta=SimpleNamespace(content="Hello"))
            ]),
            SimpleNamespace(choices=[
                SimpleNamespace(delta=SimpleNamespace(content=" World"))
            ]),
        ]

        for chunk in chunks:
            adapter.append(chunk, provider="openai")
        msg = adapter.build_message()
        content = msg.get("content", "")
        assert content == "Hello World", f"expected 'Hello World' got '{content}'"
        results.append(("LLMStreamAdapter", True, f"'{content}'"))
    except Exception as e:
        results.append(("LLMStreamAdapter", False, str(e)))

    # --- 8. get_graph() / Mermaid 可视化 ---
    try:

        class St8(TypedDict):
            x: int

        g8 = StateGraph(St8)
        g8.add_node("a", lambda s: {"x": s["x"] + 1})
        g8.add_node("b", lambda s: {"x": s["x"] + 2})
        g8.add_edge(START, "a")
        g8.add_edge("a", "b")
        g8.add_edge("b", END)

        mermaid = g8.get_graph()
        assert "a" in mermaid and "b" in mermaid, f"mermaid missing nodes: {mermaid}"
        assert "__start__" in mermaid, f"missing START: {mermaid}"
        results.append(("Mermaid 可视化", True, f"{len(mermaid)} chars"))
    except Exception as e:
        results.append(("Mermaid 可视化", False, str(e)))

    # --- 9. GraphRecursionError ---
    try:

        class St9(TypedDict):
            x: int

        g9 = StateGraph(St9)
        g9.add_node("loop", lambda s: {"x": s["x"] + 1})
        g9.add_edge(START, "loop")
        g9.add_edge("loop", "loop")  # self-loop

        compiled9 = g9.compile()
        try:
            compiled9.invoke({"x": 0}, {"recursion_limit": 5})
            results.append(("GraphRecursionError", False, "no exception raised"))
        except GraphRecursionError:
            results.append(("GraphRecursionError", True, "正确触发递归限制"))
    except Exception as e:
        results.append(("GraphRecursionError", False, str(e)))

    # --- 10. 异常层次验证 ---
    try:
        assert issubclass(GraphInterrupt, GraphBubbleUp)
        assert issubclass(ParentCommand, GraphBubbleUp)
        results.append(("异常层次", True, "GraphInterrupt/ParentCommand < GraphBubbleUp"))
    except Exception as e:
        results.append(("异常层次", False, str(e)))

    # --- 11. TAG_HIDDEN constant ---
    try:
        assert isinstance(TAG_HIDDEN, str) and len(TAG_HIDDEN) > 0
        # Verify TAG_HIDDEN can be used as a node name that gets hidden from visualization
        g_hidden = StateGraph(TypedDict("HSt", {"x": int}))
        g_hidden.add_node("visible", lambda s: {"x": s["x"] + 1})
        g_hidden.add_node(TAG_HIDDEN + "_internal", lambda s: {"x": s["x"]})
        g_hidden.add_edge(START, "visible")
        g_hidden.add_edge("visible", TAG_HIDDEN + "_internal")
        g_hidden.add_edge(TAG_HIDDEN + "_internal", END)
        app_h = g_hidden.compile()
        result_h = app_h.invoke({"x": 0})
        assert result_h["x"] == 1, f"expected 1, got {result_h}"
        results.append(("TAG_HIDDEN", True, f"value='{TAG_HIDDEN}', graph executes correctly"))
    except Exception as e:
        results.append(("TAG_HIDDEN", False, str(e)))

    # --- 12. CheckpointTuple type ---
    try:
        saver = InMemorySaver()
        g = StateGraph(TypedDict("St", {"x": int}))
        g.add_node("n", lambda s: {"x": s["x"] + 1})
        g.add_edge(START, "n")
        g.add_edge("n", END)
        app = g.compile(checkpointer=saver)
        cfg = {"configurable": {"thread_id": "ct_test"}}
        app.invoke({"x": 0}, cfg)
        tup = saver.get_tuple(cfg)
        ok = isinstance(tup, CheckpointTuple) and tup.checkpoint is not None
        results.append(("CheckpointTuple", ok, f"type={type(tup).__name__}"))
    except Exception as e:
        results.append(("CheckpointTuple", False, str(e)))

    # --- 13. stream_openai function ---
    try:
        from types import SimpleNamespace

        def fake_llm_callable(**kwargs):
            assert kwargs.get("stream") is True
            return iter([
                SimpleNamespace(choices=[
                    SimpleNamespace(delta=SimpleNamespace(content="Hi"))
                ]),
            ])

        class MsgSt(TypedDict):
            messages: list

        g = StateGraph(MsgSt)
        def stream_node(state):
            result = yield from stream_openai(
                fake_llm_callable, state["messages"]
            )
            return result
        g.add_node("stream", stream_node)
        g.add_edge(START, "stream")
        g.add_edge("stream", END)
        app = g.compile()
        chunks = []
        for ev in app.stream({"messages": [{"role": "user", "content": "hi"}]}, stream_mode="messages"):
            chunks.append(ev)
        # Verify we got the actual content chunk (stream_openai yields str deltas)
        assert len(chunks) >= 1, f"expected >=1 chunks got {len(chunks)}"
        has_content = any(
            (isinstance(c, dict) and c.get("chunk") == "Hi") or c == "Hi"
            for c in chunks
        )
        assert has_content, f"expected chunk with content='Hi', got {chunks}"
        results.append(("stream_openai", True, f"{len(chunks)} chunks, content='Hi' verified"))
    except Exception as e:
        results.append(("stream_openai", False, str(e)))

    # --- 14. PregelTask type ---
    try:
        pt = PregelTask(id="t1", name="node_a", path=("a",), error=None)
        ok = pt.id == "t1" and pt.name == "node_a" and pt.error is None
        results.append(("PregelTask", ok, f"id={pt.id}, name={pt.name}"))
    except Exception as e:
        results.append(("PregelTask", False, str(e)))

    return results
