"""场景 10：异步执行 — ainvoke/astream/abatch/async generator/max_concurrency"""

from __future__ import annotations

import asyncio
import operator
from typing import Annotated, TypedDict

from zerograph import END, START, StateGraph
from zerograph.checkpoint.memory import InMemorySaver


def run() -> list[tuple[str, bool, str]]:
    results: list[tuple[str, bool, str]] = []

    # --- 1. ainvoke 基本异步执行 ---
    try:
        class St(TypedDict):
            x: int

        async def async_inc(state):
            await asyncio.sleep(0.001)
            return {"x": state["x"] + 1}

        g = StateGraph(St)
        g.add_node("inc", async_inc)
        g.add_edge(START, "inc")
        g.add_edge("inc", END)
        compiled = g.compile()

        result = asyncio.run(compiled.ainvoke({"x": 0}))
        assert result == {"x": 1}, f"expected x=1 got {result}"
        results.append(("ainvoke 基本执行", True, f"x={result['x']}"))
    except Exception as e:
        results.append(("ainvoke 基本执行", False, str(e)))

    # --- 2. astream 异步流 ---
    try:
        g2 = StateGraph(St)
        g2.add_node("a", lambda s: {"x": s["x"] + 1})
        g2.add_node("b", lambda s: {"x": s["x"] * 2})
        g2.add_edge(START, "a")
        g2.add_edge("a", "b")
        g2.add_edge("b", END)
        compiled2 = g2.compile()

        events = []

        async def collect():
            async for ev in compiled2.astream({"x": 3}, stream_mode="updates"):
                events.append(ev)

        asyncio.run(collect())
        assert len(events) == 2, f"expected 2 events got {len(events)}"
        # Verify content: first event from "a" (x: 3+1=4), second from "b" (x: 4*2=8)
        has_a = any(isinstance(e, dict) and "a" in e and e["a"].get("x") == 4 for e in events)
        has_b = any(isinstance(e, dict) and "b" in e and e["b"].get("x") == 8 for e in events)
        assert has_a and has_b, f"expected a(x=4), b(x=8) in events: {events}"
        results.append(("astream updates", True, f"2 events, a(x=4), b(x=8) verified"))
    except Exception as e:
        results.append(("astream updates", False, str(e)))

    # --- 3. abatch 批量并发 ---
    try:
        g3 = StateGraph(St)
        g3.add_node("inc", lambda s: {"x": s["x"] + 10})
        g3.add_edge(START, "inc")
        g3.add_edge("inc", END)
        compiled3 = g3.compile()

        batch_results = asyncio.run(compiled3.abatch([{"x": 1}, {"x": 2}, {"x": 3}]))
        assert len(batch_results) == 3
        assert batch_results[0]["x"] == 11
        assert batch_results[1]["x"] == 12
        assert batch_results[2]["x"] == 13
        results.append(("abatch 批量执行", True, f"{len(batch_results)} results"))
    except Exception as e:
        results.append(("abatch 批量执行", False, str(e)))

    # --- 4. async generator 节点 ---
    try:

        def gen_node(state):
            yield {"x": state["x"] + 1}
            yield {"x": state["x"] + 2}

        g4 = StateGraph(St)
        g4.add_node("gen", gen_node)
        g4.add_edge(START, "gen")
        g4.add_edge("gen", END)
        compiled4 = g4.compile()

        msg_events = []

        async def collect_msgs():
            async for ev in compiled4.astream({"x": 0}, stream_mode="messages"):
                msg_events.append(ev)

        asyncio.run(collect_msgs())
        assert len(msg_events) == 2, f"expected 2 message chunks got {len(msg_events)}"
        results.append(("async generator 流", True, f"{len(msg_events)} chunks"))
    except Exception as e:
        results.append(("async generator 流", False, str(e)))

    # --- 5. max_concurrency 并行限制 ---
    try:
        import time

        execution_order: list[str] = []

        async def slow_a(state):
            execution_order.append("a_start")
            await asyncio.sleep(0.05)
            execution_order.append("a_end")
            return {"x": state["x"] + 1}

        async def slow_b(state):
            execution_order.append("b_start")
            await asyncio.sleep(0.05)
            execution_order.append("b_end")
            return {"x": state["x"] + 10}

        g5 = StateGraph(St)
        g5.add_node("a", slow_a)
        g5.add_node("b", slow_b)
        g5.add_edge(START, "a")
        g5.add_edge(START, "b")
        g5.add_edge("a", END)
        g5.add_edge("b", END)
        compiled5 = g5.compile()

        # With concurrency=2, both should run in parallel (~50ms not ~100ms)
        t0 = time.monotonic()
        result5 = asyncio.run(
            compiled5.ainvoke({"x": 0}, {"max_concurrency": 2})
        )
        elapsed_parallel = time.monotonic() - t0

        # Negative control: with concurrency=1, should take ~100ms (sequential)
        execution_order.clear()
        t1 = time.monotonic()
        asyncio.run(
            compiled5.ainvoke({"x": 0}, {"max_concurrency": 1})
        )
        elapsed_serial = time.monotonic() - t1

        assert elapsed_parallel < 0.15, f"parallel too slow: {elapsed_parallel:.3f}s"
        assert elapsed_serial > elapsed_parallel * 0.8, (
            f"serial ({elapsed_serial:.3f}s) not slower than parallel ({elapsed_parallel:.3f}s)"
        )
        results.append(
            ("max_concurrency 并行", True,
             f"parallel={elapsed_parallel:.3f}s, serial={elapsed_serial:.3f}s")
        )
    except Exception as e:
        results.append(("max_concurrency 并行", False, str(e)))

    # --- 6. 异步 + 检查点 ---
    try:
        class AccSt(TypedDict):
            x: Annotated[int, operator.add]

        g6 = StateGraph(AccSt)
        g6.add_node("inc", lambda s: {"x": 1})
        g6.add_edge(START, "inc")
        g6.add_edge("inc", END)
        saver6 = InMemorySaver()
        compiled6 = g6.compile(checkpointer=saver6)

        config6 = {"configurable": {"thread_id": "async_t1"}}
        r1 = asyncio.run(compiled6.ainvoke({"x": 0}, config6))
        r2 = asyncio.run(compiled6.ainvoke({"x": 0}, config6))
        assert r2["x"] == 2, f"expected x=2 got {r2}"
        results.append(("异步+检查点持久化", True, f"x: 0→{r1['x']}→{r2['x']}"))
    except Exception as e:
        results.append(("异步+检查点持久化", False, str(e)))

    # --- 7. sync/async 混合节点 ---
    try:

        def sync_node(state):
            return {"x": state["x"] + 1}

        async def async_node(state):
            return {"x": state["x"] * 2}

        g7 = StateGraph(St)
        g7.add_node("sync", sync_node)
        g7.add_node("async", async_node)
        g7.add_edge(START, "sync")
        g7.add_edge("sync", "async")
        g7.add_edge("async", END)
        compiled7 = g7.compile()

        r7 = asyncio.run(compiled7.ainvoke({"x": 5}))
        assert r7["x"] == 12, f"expected 12 got {r7['x']}"
        results.append(("sync/async 混合", True, f"x: 5→6→12"))
    except Exception as e:
        results.append(("sync/async 混合", False, str(e)))

    # --- 8. 异步子图 + interrupt ---
    try:
        from zerograph import interrupt, Command

        def child_step(state):
            val = interrupt("async_need")
            return {"x": state["x"] + val}

        child_builder = StateGraph(St)
        child_builder.add_node("child_step", child_step)
        child_builder.add_edge(START, "child_step")
        child_builder.add_edge("child_step", END)
        child = child_builder.compile()

        async def async_entry(state):
            return {"x": state["x"]}

        parent = StateGraph(St)
        parent.add_node("entry", async_entry)
        parent.add_node("sub", child)
        parent.add_edge(START, "entry")
        parent.add_edge("entry", "sub")
        parent.add_edge("sub", END)

        saver = InMemorySaver()
        app = parent.compile(checkpointer=saver)
        cfg = {"configurable": {"thread_id": "async_sub_int"}}

        # First ainvoke — should interrupt
        r1 = asyncio.run(app.ainvoke({"x": 20}, cfg))
        snap = app.get_state(cfg)
        if not snap.interrupts:
            results.append(("异步子图+interrupt", False, f"No interrupt: {snap.interrupts}"))
            return results

        # Resume
        r2 = asyncio.run(app.ainvoke(Command(resume=8), cfg))
        if r2.get("x") != 28:
            results.append(("异步子图+interrupt", False, f"Expected x=28, got {r2}"))
            return results

        results.append(("异步子图+interrupt", True, "ainvoke interrupt→resume, x=20+8=28"))
    except Exception as e:
        results.append(("异步子图+interrupt", False, str(e)))

    return results
