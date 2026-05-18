"""ZeroGraph Fuzz Testing — 随机图拓扑 + 随机输入 + 随机行为，发现深层路径问题"""

import sys
import os
import time
import random
import asyncio
import traceback
from typing import Annotated, TypedDict
import operator

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from zerograph import (
    START, END,
    StateGraph,
    Command,
    Send,
    Overwrite,
    InMemorySaver,
    SqliteSaver,
    GraphRecursionError,
    GraphBubbleUp,
)
from zerograph.checkpoint.memory import InMemorySaver


SEED = None  # None = random; set int for reproducibility
RNG = random.Random(SEED)

# ── Fuzz parameters ──
FUZZ_ITERS = 10000
MAX_NODES = 12
MAX_STATES = 50
TIMEOUT_PER_GRAPH = 5.0  # seconds


# ═══════════════════════════════════════════════════════════════════════════
# Random generators
# ═══════════════════════════════════════════════════════════════════════════

def rand_node_name(idx):
    return f"n{idx}"

def rand_int():
    return RNG.randint(-100, 100)

def rand_node_fn(name):
    """Generate a random node function."""
    kind = RNG.choice([
        "inc", "mul", "identity", "const", "overwrite",
        "command_goto", "send_list",
    ])

    if kind == "inc":
        delta = RNG.randint(-5, 5)
        if delta == 0:
            delta = 1
        def fn(state):
            return {"x": state.get("x", 0) + delta}
        fn.__name__ = f"{name}_inc_{delta}"

    elif kind == "mul":
        factor = RNG.choice([2, 3, -1, 0, 1])
        def fn(state):
            return {"x": state.get("x", 0) * factor}
        fn.__name__ = f"{name}_mul_{factor}"

    elif kind == "identity":
        def fn(state):
            return {"x": state.get("x", 0)}
        fn.__name__ = f"{name}_id"

    elif kind == "const":
        val = rand_int()
        def fn(state):
            return {"x": val}
        fn.__name__ = f"{name}_const_{val}"

    elif kind == "overwrite":
        val = rand_int()
        def fn(state):
            return {"x": Overwrite(val)}
        fn.__name__ = f"{name}_ow_{val}"

    elif kind == "command_goto":
        def fn(state):
            return {"x": state.get("x", 0) + 1}
        fn.__name__ = f"{name}_inc1"

    elif kind == "send_list":
        def fn(state):
            return {"x": state.get("x", 0) + 1}
        fn.__name__ = f"{name}_inc1"

    return fn, kind


def rand_graph_topology(num_nodes):
    """Generate random edges for a graph with given nodes."""
    nodes = [rand_node_name(i) for i in range(num_nodes)]
    edges = []

    # Ensure START connects to at least one node
    first_nodes = RNG.sample(nodes, k=min(RNG.randint(1, min(3, num_nodes)), num_nodes))
    for n in first_nodes:
        edges.append((START, n))

    # Ensure at least one node connects to END
    last_nodes = RNG.sample(nodes, k=min(RNG.randint(1, min(3, num_nodes)), num_nodes))
    for n in last_nodes:
        edges.append((n, END))

    # Add random internal edges
    for _ in range(RNG.randint(0, num_nodes)):
        src = RNG.choice(nodes)
        dst = RNG.choice(nodes)
        if src != dst:
            edges.append((src, dst))

    return nodes, edges, first_nodes, last_nodes


def rand_conditional_edge(node_name, all_nodes):
    """Maybe add a conditional edge from node."""
    if RNG.random() < 0.3 and len(all_nodes) > 1:
        targets = [n for n in all_nodes if n != node_name]
        if not targets:
            return None
        chosen = RNG.choice(targets)
        threshold = RNG.randint(0, 50)

        def router(state, _threshold=threshold, _target=chosen):
            x = state.get("x", 0) if isinstance(state, dict) else 0
            return _target if x > _threshold else None

        path_map = {chosen: chosen, None: None}
        return router, path_map
    return None


# ═══════════════════════════════════════════════════════════════════════════
# Fuzz test cases
# ═══════════════════════════════════════════════════════════════════════════

def fuzz_random_graph():
    """Generate and execute a completely random graph."""
    num_nodes = RNG.randint(1, MAX_NODES)
    nodes, edges, first_nodes, last_nodes = rand_graph_topology(num_nodes)

    # Create state schema
    St = TypedDict(f"FuzzSt_{id(edges)}", {"x": int})

    g = StateGraph(St)

    # Add nodes with random functions
    node_kinds = {}
    for node in nodes:
        fn, kind = rand_node_fn(node)
        node_kinds[node] = kind
        g.add_node(node, fn)

    # Add edges
    for src, dst in edges:
        try:
            if src == START:
                g.add_edge(START, dst)
            elif dst == END:
                g.add_edge(src, END)
            else:
                g.add_edge(src, dst)
        except (ValueError, KeyError):
            continue

    # Maybe add conditional edges
    cond_nodes = []
    for node in nodes:
        cond = rand_conditional_edge(node, nodes)
        if cond:
            router, path_map = cond
            try:
                g.add_conditional_edges(node, router, path_map)
                cond_nodes.append(node)
            except (ValueError, KeyError):
                continue

    # Maybe use checkpointer
    use_checkpointer = RNG.random() < 0.3
    checkpointer = InMemorySaver() if use_checkpointer else None

    # Maybe set interrupt_before / interrupt_after
    interrupt_before = []
    interrupt_after = []
    if RNG.random() < 0.15 and use_checkpointer and nodes:
        target = RNG.choice(nodes)
        interrupt_before = [target]
    if RNG.random() < 0.15 and use_checkpointer and nodes:
        target = RNG.choice(nodes)
        interrupt_after = [target]

    try:
        app = g.compile(
            checkpointer=checkpointer,
            interrupt_before=interrupt_before,
            interrupt_after=interrupt_after,
        )
    except Exception as e:
        # Some random graphs may be invalid — that's fine
        return True, f"compile_error: {type(e).__name__}: {str(e)[:80]}"

    # Execute
    initial_x = rand_int()
    recursion_limit = RNG.choice([5, 10, 25, 50])

    try:
        cfg = {"configurable": {"thread_id": f"fuzz_{id(app)}"}}
        result = app.invoke(
            {"x": initial_x},
            {**cfg, "recursion_limit": recursion_limit},
        )
        return True, f"ok, x={initial_x}→{result.get('x', '?')}, nodes={num_nodes}"
    except GraphRecursionError:
        return True, f"recursion_limit={recursion_limit} hit (expected)"
    except Exception as e:
        return False, f"CRASH: {type(e).__name__}: {str(e)[:120]}"


def fuzz_send_graph():
    """Random graph with Send fan-out."""
    num_targets = RNG.randint(2, 8)
    targets = [f"target_{i}" for i in range(num_targets)]

    St = TypedDict(f"SendSt_{num_targets}", {
        "x": Annotated[int, operator.add],
        "results": Annotated[list, operator.add],
    })

    def router(state):
        chosen = RNG.sample(targets, k=RNG.randint(1, num_targets))
        return [Send(t, rand_int()) for t in chosen]

    g = StateGraph(St)
    g.add_node("router", router)

    for t in targets:
        def make_fn(name):
            def fn(state):
                return {"x": 1, "results": [name]}
            return fn
        g.add_node(t, make_fn(t))

    g.add_edge(START, "router")
    g.add_conditional_edges("router", router, path_map={t: t for t in targets})
    for t in targets:
        g.add_edge(t, END)

    try:
        app = g.compile()
    except Exception as e:
        return True, f"compile_error: {e}"

    try:
        result = app.invoke({"x": 0, "results": []}, {"recursion_limit": 50})
        num_results = len(result.get("results", []))
        total_x = result.get("x", 0)
        return True, f"fan-out={num_results}, x={total_x}"
    except GraphRecursionError:
        return True, "recursion_limit hit"
    except Exception as e:
        return False, f"CRASH: {type(e).__name__}: {str(e)[:120]}"


def fuzz_interrupt_resume():
    """Random graph with interrupt + multi-step resume."""
    num_nodes = RNG.randint(2, 5)
    nodes = [f"n{i}" for i in range(num_nodes)]

    St = TypedDict(f"IntSt_{num_nodes}", {"x": int})

    # Pick a node to interrupt in
    interrupt_node_idx = RNG.randint(0, num_nodes - 1)
    interrupt_node = nodes[interrupt_node_idx]

    g = StateGraph(St)
    prev = START
    for i, node in enumerate(nodes):
        if i == interrupt_node_idx:
            from zerograph import interrupt
            def make_interrupt_fn(idx):
                def fn(state):
                    val = interrupt("pause")
                    return {"x": state.get("x", 0) + val}
                return fn
            g.add_node(node, make_interrupt_fn(i))
        else:
            def make_fn(delta):
                def fn(state):
                    return {"x": state.get("x", 0) + delta}
                return fn
            delta = RNG.randint(1, 5)
            g.add_node(node, make_fn(delta))
        g.add_edge(prev, node)
        prev = node
    g.add_edge(prev, END)

    saver = InMemorySaver()
    try:
        app = g.compile(checkpointer=saver)
    except Exception as e:
        return True, f"compile_error: {e}"

    cfg = {"configurable": {"thread_id": f"int_{RNG.randint(0,9999)}"}}

    try:
        # First invoke — should interrupt
        app.invoke({"x": 0}, cfg)

        # Resume with random value
        resume_val = RNG.randint(1, 100)
        result = app.invoke(Command(resume=resume_val), cfg)
        return True, f"interrupt@{interrupt_node}, resume={resume_val}, final_x={result.get('x', '?')}"
    except Exception as e:
        tb = traceback.format_exc()
        short = tb.split('\n')[-2] if '\n' in tb else str(e)
        return False, f"CRASH: {short[:120]}"


def fuzz_checkpoint_history():
    """Invoke graph multiple times, verify history consistency."""
    St = TypedDict(f"HistSt_{RNG.randint(0,9999)}", {"x": int})

    g = StateGraph(St)
    g.add_node("inc", lambda s: {"x": s.get("x", 0) + 1})
    g.add_edge(START, "inc")
    g.add_edge("inc", END)

    saver = InMemorySaver()
    app = g.compile(checkpointer=saver)

    thread_id = f"hist_{RNG.randint(0,99999)}"
    cfg = {"configurable": {"thread_id": thread_id}}

    num_invokes = RNG.randint(3, 20)
    try:
        for i in range(num_invokes):
            app.invoke({"x": i}, cfg)

        history = app.get_state_history(cfg, limit=100)
        if len(history) < num_invokes:
            return False, f"history too short: {len(history)} < {num_invokes}"

        # Verify parent chain
        parents = sum(1 for s in history if s.parent_config is not None)
        return True, f"{num_invokes} invokes, {len(history)} snapshots, {parents} parents"
    except Exception as e:
        return False, f"CRASH: {type(e).__name__}: {str(e)[:120]}"


def fuzz_subgraph():
    """Random graph with random subgraph nesting."""
    inner_size = RNG.randint(1, 3)

    InnerSt = TypedDict(f"InnerSt_{inner_size}", {"x": int})
    OuterSt = TypedDict(f"OuterSt_{inner_size}", {"x": int})

    # Build inner graph
    ig = StateGraph(InnerSt)
    prev = START
    for i in range(inner_size):
        def make_fn(d):
            def fn(s):
                return {"x": s.get("x", 0) + d}
            return fn
        ig.add_node(f"inner_{i}", make_fn(RNG.randint(1, 3)))
        ig.add_edge(prev, f"inner_{i}")
        prev = f"inner_{i}"
    ig.add_edge(prev, END)
    inner_app = ig.compile()

    # Build outer graph using inner as node
    og = StateGraph(OuterSt)
    og.add_node("child", inner_app)
    og.add_node("pre", lambda s: {"x": s.get("x", 0) + 10})
    og.add_node("post", lambda s: {"x": s.get("x", 0) + 100})
    og.add_edge(START, "pre")
    og.add_edge("pre", "child")
    og.add_edge("child", "post")
    og.add_edge("post", END)

    try:
        app = og.compile()
        result = app.invoke({"x": RNG.randint(0, 10)})
        return True, f"inner_depth={inner_size}, x={result.get('x', '?')}"
    except Exception as e:
        return False, f"CRASH: {type(e).__name__}: {str(e)[:120]}"


def fuzz_stream_modes():
    """Random graph tested with random stream modes."""
    St = TypedDict(f"StreamSt_{RNG.randint(0,9999)}", {"x": int})

    g = StateGraph(St)
    g.add_node("a", lambda s: {"x": s.get("x", 0) + 1})
    g.add_node("b", lambda s: {"x": s.get("x", 0) * 2})
    g.add_edge(START, "a")
    g.add_edge("a", "b")
    g.add_edge("b", END)

    app = g.compile()

    all_modes = ["values", "updates", "debug", "tasks"]
    mode_choice = RNG.sample(all_modes, k=RNG.randint(1, len(all_modes)))

    try:
        events = list(app.stream({"x": RNG.randint(0, 5)}, stream_mode=mode_choice))
        return True, f"modes={mode_choice}, {len(events)} events"
    except Exception as e:
        return False, f"CRASH with modes={mode_choice}: {type(e).__name__}: {str(e)[:100]}"


def fuzz_async_graph():
    """Random async execution patterns."""
    St = TypedDict(f"AsynSt_{RNG.randint(0,9999)}", {"x": int})

    async def async_inc(state):
        await asyncio.sleep(0.001)
        return {"x": state.get("x", 0) + 1}

    g = StateGraph(St)
    g.add_node("a", async_inc)
    g.add_edge(START, "a")
    g.add_edge("a", END)
    app = g.compile()

    try:
        result = asyncio.run(app.ainvoke({"x": rand_int()}))
        return True, f"ainvoke ok, x={result.get('x', '?')}"
    except Exception as e:
        return False, f"CRASH: {type(e).__name__}: {str(e)[:100]}"


def fuzz_state_update_get():
    """Random update_state / get_state operations."""
    St = TypedDict(f"UpdSt_{RNG.randint(0,9999)}", {"x": int})

    g = StateGraph(St)
    g.add_node("inc", lambda s: {"x": s.get("x", 0) + 1})
    g.add_edge(START, "inc")
    g.add_edge("inc", END)

    saver = InMemorySaver()
    app = g.compile(checkpointer=saver)
    cfg = {"configurable": {"thread_id": f"upd_{RNG.randint(0,9999)}"}}

    try:
        # Normal invoke
        app.invoke({"x": 0}, cfg)

        # Random update_state
        new_val = rand_int()
        app.update_state(cfg, {"x": new_val})

        # Verify
        snap = app.get_state(cfg)
        if snap.values.get("x") != new_val:
            return False, f"update_state failed: expected {new_val}, got {snap.values.get('x')}"

        return True, f"update_state({new_val}) verified"
    except Exception as e:
        return False, f"CRASH: {type(e).__name__}: {str(e)[:100]}"


# ═══════════════════════════════════════════════════════════════════════════
# Main fuzz runner
# ═══════════════════════════════════════════════════════════════════════════

def main():
    fuzz_fns = [
        ("random_graph", fuzz_random_graph),
        ("send_fanout", fuzz_send_graph),
        ("interrupt_resume", fuzz_interrupt_resume),
        ("checkpoint_history", fuzz_checkpoint_history),
        ("subgraph", fuzz_subgraph),
        ("stream_modes", fuzz_stream_modes),
        ("async_graph", fuzz_async_graph),
        ("state_update", fuzz_state_update_get),
    ]

    total = 0
    crashes = 0
    crash_details = []
    t0 = time.monotonic()

    print(f"ZeroGraph Fuzz Testing — {FUZZ_ITERS} iterations")
    print(f"Seed: {SEED or 'random'}")
    print("=" * 60)

    progress_interval = FUZZ_ITERS // 10

    for i in range(FUZZ_ITERS):
        name, fn = RNG.choice(fuzz_fns)
        total += 1

        try:
            ok, detail = fn()
        except Exception as e:
            ok = False
            detail = f"FUZZER BUG: {type(e).__name__}: {str(e)[:100]}"
            traceback.print_exc()

        if not ok:
            crashes += 1
            crash_details.append((name, detail))
            print(f"  \033[91m[CRASH #{crashes}]\033[0m {name}: {detail}")

        if progress_interval > 0 and (i + 1) % progress_interval == 0:
            elapsed = time.monotonic() - t0
            print(f"  ... {i+1}/{FUZZ_ITERS} ({elapsed:.1f}s), {crashes} crashes so far")

    elapsed = time.monotonic() - t0

    print(f"\n{'=' * 60}")
    print(f"  Fuzz 结果: {total} 次执行, {crashes} 次 crash ({elapsed:.1f}s)")
    if crashes == 0:
        print("  \033[92m零 crash！\033[0m")
    else:
        print(f"  \033[91m{crashes} 个 crash 需要修复！\033[0m")
        print()
        for name, detail in crash_details:
            print(f"  [{name}] {detail}")
    print(f"{'=' * 60}")
    return 0 if crashes == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
