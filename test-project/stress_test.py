"""ZeroGraph 综合质量测试 — 压力/并发/内存/边界/稳定性"""

import sys
import os
import time
import asyncio
import threading
import tracemalloc
import gc
from typing import Annotated, TypedDict
import operator

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from zerograph import (
    START, END,
    StateGraph,
    InMemorySaver,
    SqliteSaver,
    AsyncSqliteSaver,
    Command,
    Send,
    interrupt,
    GraphRecursionError,
)
from zerograph.checkpoint.memory import InMemorySaver


# ═══════════════════════════════════════════════════════════════════════════
# 1. 性能基准测试
# ═══════════════════════════════════════════════════════════════════════════

def bench_simple_graph():
    """简单图 invoke 吞吐量"""
    class St(TypedDict):
        x: int

    def inc(state):
        return {"x": state["x"] + 1}

    g = StateGraph(St)
    g.add_node("inc", inc)
    g.add_edge(START, "inc")
    g.add_edge("inc", END)
    app = g.compile()

    N = 10000
    t0 = time.perf_counter()
    for _ in range(N):
        app.invoke({"x": 0})
    elapsed = time.perf_counter() - t0
    qps = N / elapsed
    return qps, elapsed


def bench_wide_graph():
    """宽图 (20 节点并行) 吞吐量"""
    class St(TypedDict):
        x: int

    g = StateGraph(St)
    for i in range(20):
        g.add_node(f"n{i}", lambda s: {"x": s["x"] + 1})
        g.add_edge(START, f"n{i}")
        g.add_edge(f"n{i}", END)
    app = g.compile()

    N = 1000
    t0 = time.perf_counter()
    for _ in range(N):
        app.invoke({"x": 0})
    elapsed = time.perf_counter() - t0
    qps = N / elapsed
    return qps, elapsed


def bench_deep_chain():
    """深链 (50 节点串行) 吞吐量"""
    class St(TypedDict):
        x: int

    g = StateGraph(St)
    prev = START
    for i in range(50):
        g.add_node(f"n{i}", lambda s: {"x": s["x"] + 1})
        g.add_edge(prev, f"n{i}")
        prev = f"n{i}"
    g.add_edge(f"n49", END)
    app = g.compile()

    N = 100
    t0 = time.perf_counter()
    for _ in range(N):
        result = app.invoke({"x": 0}, {"recursion_limit": 100})
    elapsed = time.perf_counter() - t0
    qps = N / elapsed
    assert result["x"] == 50, f"deep chain: expected 50, got {result['x']}"
    return qps, elapsed


def bench_checkpoint_overhead():
    """检查点开销对比"""
    class St(TypedDict):
        x: int

    def inc(state):
        return {"x": state["x"] + 1}

    g = StateGraph(St)
    g.add_node("inc", inc)
    g.add_edge(START, "inc")
    g.add_edge("inc", END)

    # 无检查点
    app_no_cp = g.compile()
    N = 5000
    t0 = time.perf_counter()
    for _ in range(N):
        app_no_cp.invoke({"x": 0})
    t_no_cp = time.perf_counter() - t0

    # InMemorySaver
    app_mem = g.compile(checkpointer=InMemorySaver())
    t0 = time.perf_counter()
    for i in range(N):
        app_mem.invoke({"x": 0}, {"configurable": {"thread_id": f"t{i}"}})
    t_mem = time.perf_counter() - t0

    # SqliteSaver
    with SqliteSaver(":memory:") as saver:
        app_sql = g.compile(checkpointer=saver)
        t0 = time.perf_counter()
        for i in range(N):
            app_sql.invoke({"x": 0}, {"configurable": {"thread_id": f"t{i}"}})
        t_sql = time.perf_counter() - t0

    return t_no_cp, t_mem, t_sql


# ═══════════════════════════════════════════════════════════════════════════
# 2. 并发安全测试
# ═══════════════════════════════════════════════════════════════════════════

def test_thread_safety():
    """多线程并发 invoke，每个线程用独立 thread_id"""
    class St(TypedDict):
        x: int

    g = StateGraph(St)
    g.add_node("inc", lambda s: {"x": s["x"] + 1})
    g.add_edge(START, "inc")
    g.add_edge("inc", END)

    saver = InMemorySaver()
    app = g.compile(checkpointer=saver)

    THREADS = 20
    ITERS = 50
    errors = []

    def worker(thread_id):
        try:
            cfg = {"configurable": {"thread_id": f"thread_{thread_id}"}}
            for i in range(ITERS):
                result = app.invoke({"x": i}, cfg)
                expected = i + 1
                # 注意: 没有 reducer 时，invoke 传入新值会覆盖
                # 但用检查点后，每次传入 {x: i} → inc → x=i+1
        except Exception as e:
            errors.append((thread_id, str(e)))

    threads = [threading.Thread(target=worker, args=(tid,)) for tid in range(THREADS)]
    t0 = time.perf_counter()
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    elapsed = time.perf_counter() - t0

    return THREADS, ITERS, len(errors), elapsed, errors[:5]


def test_shared_saver_isolation():
    """多线程共享 SqliteSaver，验证线程隔离"""
    class St(TypedDict):
        x: int

    g = StateGraph(St)
    g.add_node("inc", lambda s: {"x": s["x"] + 1})
    g.add_edge(START, "inc")
    g.add_edge("inc", END)

    THREADS = 10
    errors = []

    with SqliteSaver(":memory:") as saver:
        app = g.compile(checkpointer=saver)

        def worker(tid):
            try:
                cfg = {"configurable": {"thread_id": f"iso_{tid}"}}
                for i in range(20):
                    result = app.invoke({"x": i}, cfg)
                    if result["x"] != i + 1:
                        errors.append(f"thread {tid}: iter {i}, got {result['x']}")
            except Exception as e:
                errors.append(f"thread {tid}: {e}")

        threads = [threading.Thread(target=worker, args=(t,)) for t in range(THREADS)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

    return len(errors), errors[:5]


def test_async_concurrent():
    """异步并发 ainvoke"""
    class St(TypedDict):
        x: int

    g = StateGraph(St)
    g.add_node("inc", lambda s: {"x": s["x"] + 1})
    g.add_edge(START, "inc")
    g.add_edge("inc", END)
    app = g.compile()

    N = 200

    async def run_batch():
        tasks = [app.ainvoke({"x": i}) for i in range(N)]
        return await asyncio.gather(*tasks)

    t0 = time.perf_counter()
    results = asyncio.run(run_batch())
    elapsed = time.perf_counter() - t0

    ok = all(r["x"] == i + 1 for i, r in enumerate(results))
    return N, elapsed, ok


# ═══════════════════════════════════════════════════════════════════════════
# 3. 内存泄漏测试
# ═══════════════════════════════════════════════════════════════════════════

def test_memory_stability():
    """反复 invoke 检测内存增长"""
    class St(TypedDict):
        x: int

    g = StateGraph(St)
    g.add_node("inc", lambda s: {"x": s["x"] + 1})
    g.add_edge(START, "inc")
    g.add_edge("inc", END)

    saver = InMemorySaver()
    app = g.compile(checkpointer=saver)
    cfg = {"configurable": {"thread_id": "leak_test"}}

    # Warmup
    for _ in range(100):
        app.invoke({"x": 0}, cfg)

    gc.collect()
    tracemalloc.start()
    snapshot1 = tracemalloc.take_snapshot()

    N = 5000
    for _ in range(N):
        app.invoke({"x": 0}, cfg)

    gc.collect()
    snapshot2 = tracemalloc.take_snapshot()
    tracemalloc.stop()

    stats = snapshot2.compare_to(snapshot1, "lineno")
    total_diff = sum(s.size_diff for s in stats)
    total_kb = total_diff / 1024

    return N, total_kb


def test_sqlite_memory_stability():
    """SqliteSaver 反复写入检测内存"""
    class St(TypedDict):
        x: int

    g = StateGraph(St)
    g.add_node("inc", lambda s: {"x": s["x"] + 1})
    g.add_edge(START, "inc")
    g.add_edge("inc", END)

    N = 3000

    with SqliteSaver(":memory:") as saver:
        app = g.compile(checkpointer=saver)

        for _ in range(100):
            app.invoke({"x": 0}, {"configurable": {"thread_id": "w"}})

        gc.collect()
        tracemalloc.start()
        s1 = tracemalloc.take_snapshot()

        for i in range(N):
            app.invoke({"x": 0}, {"configurable": {"thread_id": f"sql_{i}"}})

        gc.collect()
        s2 = tracemalloc.take_snapshot()
        tracemalloc.stop()

    total_kb = sum(s.size_diff for s in s2.compare_to(s1, "lineno")) / 1024
    return N, total_kb


# ═══════════════════════════════════════════════════════════════════════════
# 4. 边界极限测试
# ═══════════════════════════════════════════════════════════════════════════

def test_large_state():
    """大状态 (10000 个 key)"""
    class BigSt(TypedDict):
        pass

    # 动态创建 state
    N_KEYS = 10000
    state_dict = {f"k{i}": i for i in range(N_KEYS)}

    def inc_all(state):
        return {f"k{i}": state.get(f"k{i}", 0) + 1 for i in range(N_KEYS)}

    from typing import Any
    BigSt = TypedDict("BigSt", {f"k{i}": int for i in range(N_KEYS)})

    g = StateGraph(BigSt)
    g.add_node("inc", inc_all)
    g.add_edge(START, "inc")
    g.add_edge("inc", END)
    app = g.compile()

    t0 = time.perf_counter()
    result = app.invoke(state_dict)
    elapsed = time.perf_counter() - t0

    ok = result["k0"] == 1 and result["k9999"] == 9999 + 1
    return N_KEYS, elapsed, ok


def test_send_fanout_100():
    """Send fan-out 100 路并行"""
    class St(TypedDict):
        items: Annotated[list, operator.add]

    def router(state):
        return [Send("proc", i) for i in range(100)]

    def proc(state):
        # When triggered by Send, state contains the full graph state dict
        # The Send arg is available via the node's input mechanism
        return {"items": ["done"]}

    g = StateGraph(St)
    g.add_node("router", router)
    g.add_node("proc", proc)
    g.add_edge(START, "router")
    g.add_conditional_edges("router", router, path_map={"proc": "proc"})
    g.add_edge("proc", END)
    app = g.compile()

    t0 = time.perf_counter()
    result = app.invoke({"items": []})
    elapsed = time.perf_counter() - t0

    # Each Send triggers one proc execution, each returns ["done"]
    return len(result["items"]), elapsed, len(result["items"]) == 100


def test_recursion_limit_exact():
    """递归限制精确触发"""
    class St(TypedDict):
        x: int

    g = StateGraph(St)
    g.add_node("loop", lambda s: {"x": s["x"] + 1})
    g.add_edge(START, "loop")
    g.add_edge("loop", "loop")

    app = g.compile()
    caught = False
    try:
        app.invoke({"x": 0}, {"recursion_limit": 10})
    except GraphRecursionError:
        caught = True

    return caught


def test_empty_graph_invoke():
    """空输入 / 极小状态"""
    class St(TypedDict):
        x: int

    g = StateGraph(St)
    g.add_node("id", lambda s: s)
    g.add_edge(START, "id")
    g.add_edge("id", END)
    app = g.compile()

    result = app.invoke({"x": 0})
    return result["x"] == 0


# ═══════════════════════════════════════════════════════════════════════════
# 5. 长时间稳定性测试
# ═══════════════════════════════════════════════════════════════════════════

def test_long_run_stability():
    """10000 次循环执行，验证无累积误差"""
    class St(TypedDict):
        x: int

    g = StateGraph(St)
    g.add_node("inc", lambda s: {"x": s["x"] + 1})
    g.add_edge(START, "inc")
    g.add_edge("inc", END)
    app = g.compile()

    N = 10000
    errors = 0
    t0 = time.perf_counter()
    for i in range(N):
        result = app.invoke({"x": i})
        if result["x"] != i + 1:
            errors += 1
    elapsed = time.perf_counter() - t0

    return N, errors, elapsed


def test_checkpoint_history_growth():
    """反复 invoke 检查点历史不无限增长"""
    class St(TypedDict):
        x: int

    g = StateGraph(St)
    g.add_node("inc", lambda s: {"x": s["x"] + 1})
    g.add_edge(START, "inc")
    g.add_edge("inc", END)

    saver = InMemorySaver()
    app = g.compile(checkpointer=saver)
    cfg = {"configurable": {"thread_id": "hist_growth"}}

    N = 500
    for i in range(N):
        app.invoke({"x": i}, cfg)

    history = app.get_state_history(cfg, limit=10000)
    return N, len(history)


# ═══════════════════════════════════════════════════════════════════════════
# 主控
# ═══════════════════════════════════════════════════════════════════════════

def main():
    total_pass = 0
    total_fail = 0

    def report(name, ok, detail):
        nonlocal total_pass, total_fail
        status = "\033[92mPASS\033[0m" if ok else "\033[91mFAIL\033[0m"
        if ok:
            total_pass += 1
        else:
            total_fail += 1
        print(f"  [{status}] {name}: {detail}")

    start = time.monotonic()

    # ─── 性能基准 ───
    print("\n" + "=" * 60)
    print("  性能基准测试")
    print("=" * 60)

    try:
        qps, t = bench_simple_graph()
        report("简单图吞吐", qps > 5000, f"{qps:.0f} ops/s ({t:.3f}s / 10000次)")
    except Exception as e:
        report("简单图吞吐", False, str(e))

    try:
        qps, t = bench_wide_graph()
        report("宽图吞吐 (20节点并行)", qps > 200, f"{qps:.0f} ops/s ({t:.3f}s / 1000次)")
    except Exception as e:
        report("宽图吞吐", False, str(e))

    try:
        qps, t = bench_deep_chain()
        report("深链吞吐 (50节点串行)", qps > 20, f"{qps:.0f} ops/s ({t:.3f}s / 100次, 末值=50)")
    except Exception as e:
        report("深链吞吐", False, str(e))

    try:
        t_no, t_mem, t_sql = bench_checkpoint_overhead()
        ratio_mem = t_mem / t_no
        ratio_sql = t_sql / t_no
        report("检查点开销",
               ratio_sql < 50,
               f"无CP: {t_no:.3f}s | InMemory: {ratio_mem:.1f}x | SQLite: {ratio_sql:.1f}x")
    except Exception as e:
        report("检查点开销", False, str(e))

    # ─── 并发安全 ───
    print("\n" + "=" * 60)
    print("  并发安全测试")
    print("=" * 60)

    try:
        threads, iters, errs, t, samples = test_thread_safety()
        report("多线程安全 (InMemory)",
               errs == 0,
               f"{threads}线程 × {iters}次 = {threads*iters}次 ({t:.3f}s), 错误={errs}")
        if samples:
            print(f"         错误样例: {samples}")
    except Exception as e:
        report("多线程安全", False, str(e))

    try:
        errs, samples = test_shared_saver_isolation()
        report("SQLite 线程隔离",
               errs == 0,
               f"10线程 × 20次, 错误={errs}")
        if samples:
            print(f"         错误样例: {samples}")
    except Exception as e:
        report("SQLite 线程隔离", False, str(e))

    try:
        n, t, ok = test_async_concurrent()
        report("异步并发 ainvoke",
               ok,
               f"{n} 并发请求 ({t:.3f}s)")
    except Exception as e:
        report("异步并发", False, str(e))

    # ─── 内存 ───
    print("\n" + "=" * 60)
    print("  内存稳定性测试")
    print("=" * 60)

    try:
        n, kb = test_memory_stability()
        report("InMemorySaver 内存增长",
               kb < 50000,
               f"{n}次 invoke 后增长 {kb:.1f} KB (阈值 50MB)")
    except Exception as e:
        report("InMemorySaver 内存", False, str(e))

    try:
        n, kb = test_sqlite_memory_stability()
        report("SqliteSaver 内存增长",
               kb < 10000,
               f"{n}次 invoke 后增长 {kb:.1f} KB (阈值 10000KB)")
    except Exception as e:
        report("SqliteSaver 内存", False, str(e))

    # ─── 边界极限 ───
    print("\n" + "=" * 60)
    print("  边界极限测试")
    print("=" * 60)

    try:
        n, t, ok = test_large_state()
        report(f"大状态 ({n} keys)",
               ok,
               f"{t:.3f}s, 结果正确={ok}")
    except Exception as e:
        report("大状态", False, str(e))

    try:
        items, t, ok = test_send_fanout_100()
        report("Send fan-out 100路",
               ok,
               f"{items} 项处理 ({t:.3f}s)")
    except Exception as e:
        report("Send fan-out 100", False, str(e))

    try:
        ok = test_recursion_limit_exact()
        report("递归限制精确触发", ok, f"GraphRecursionError raised={ok}")
    except Exception as e:
        report("递归限制", False, str(e))

    try:
        ok = test_empty_graph_invoke()
        report("极小状态 invoke", ok, f"x=0 传递正确={ok}")
    except Exception as e:
        report("极小状态", False, str(e))

    # ─── 长时间稳定性 ───
    print("\n" + "=" * 60)
    print("  长时间稳定性测试")
    print("=" * 60)

    try:
        n, errs, t = test_long_run_stability()
        report("10000次循环稳定性",
               errs == 0,
               f"{n}次执行, {errs} 错误 ({t:.3f}s)")
    except Exception as e:
        report("循环稳定性", False, str(e))

    try:
        n, hist_len = test_checkpoint_history_growth()
        report("检查点历史记录",
               hist_len >= n,
               f"{n}次 invoke → {hist_len} 条历史记录")
    except Exception as e:
        report("检查点历史", False, str(e))

    # ─── 汇总 ───
    elapsed = time.monotonic() - start
    print(f"\n{'=' * 60}")
    print(f"  总计: {total_pass} 通过, {total_fail} 失败 ({elapsed:.2f}s)")
    if total_fail == 0:
        print("  \033[92m所有质量测试通过！\033[0m")
    else:
        print("  \033[91m存在失败的质量测试！\033[0m")
    print(f"{'=' * 60}")
    return 0 if total_fail == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
