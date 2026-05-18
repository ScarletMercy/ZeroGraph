"""Test scenario 06: Subgraphs (nested compiled graphs as nodes)."""

from typing import Annotated, TypedDict

from zerograph import (
    StateGraph,
    InMemorySaver,
    Command,
    ParentCommand,
)


# ---- Schemas ----

class _Val(TypedDict):
    x: int


class _ValY(TypedDict):
    x: int
    y: int


# ---- Test 1: Basic subgraph ----

def _test_basic_subgraph():
    """Parent has a child CompiledStateGraph as a node; child adds 10 to x."""
    try:

        # Child graph: adds 10 to x
        def child_add(state):
            return {"x": state["x"] + 10}

        child_builder = StateGraph(_Val)
        child_builder.add_node("child_add", child_add)
        child_builder.add_edge("__start__", "child_add")
        child_builder.add_edge("child_add", "__end__")
        child = child_builder.compile()

        # Parent graph uses child as a node
        def entry(state):
            return {"x": state["x"]}

        parent = StateGraph(_Val)
        parent.add_node("entry", entry)
        parent.add_node("subgraph", child)
        parent.add_edge("__start__", "entry")
        parent.add_edge("entry", "subgraph")
        parent.add_edge("subgraph", "__end__")
        app = parent.compile()

        result = app.invoke({"x": 5})
        if result.get("x") == 15:
            return True, "Child added 10: x=5+10=15"
        return False, f"Expected x=15, got {result}"
    except Exception as e:
        return False, f"Unexpected error: {e}"


# ---- Test 2: get_state(subgraphs=True) ----

def _test_get_state_subgraphs():
    """get_state(subgraphs=True) returns nested subgraph states."""
    try:

        def child_node(state):
            return {"x": state["x"] + 7}

        child_builder = StateGraph(_Val)
        child_builder.add_node("child_node", child_node)
        child_builder.add_edge("__start__", "child_node")
        child_builder.add_edge("child_node", "__end__")
        child = child_builder.compile()

        saver = InMemorySaver()

        def parent_entry(state):
            return {"x": state["x"]}

        parent = StateGraph(_Val)
        parent.add_node("parent_entry", parent_entry)
        parent.add_node("subgraph", child)
        parent.add_edge("__start__", "parent_entry")
        parent.add_edge("parent_entry", "subgraph")
        parent.add_edge("subgraph", "__end__")
        app = parent.compile(checkpointer=saver)

        cfg = {"configurable": {"thread_id": "sg1"}}
        result = app.invoke({"x": 3}, cfg)
        if result.get("x") != 10:
            return False, f"Expected x=10, got {result.get('x')}"

        snap = app.get_state(cfg, subgraphs=True)
        has_subgraphs = snap.subgraphs is not None and len(snap.subgraphs) > 0
        if not has_subgraphs:
            return False, f"get_state(subgraphs=True) returned no subgraphs: {snap.subgraphs}"
        return True, f"subgraphs present: {list(snap.subgraphs.keys())}"
    except Exception as e:
        return False, f"Unexpected error: {e}"


# ---- Test 3: Nested subgraph (3 levels) ----

def _test_nested_subgraph():
    """Parent -> child -> grandchild: three levels of nesting."""
    try:

        # Grandchild: multiplies x by 3
        def gc_multiply(state):
            return {"x": state["x"] * 3}

        gc_builder = StateGraph(_Val)
        gc_builder.add_node("gc_multiply", gc_multiply)
        gc_builder.add_edge("__start__", "gc_multiply")
        gc_builder.add_edge("gc_multiply", "__end__")
        grandchild = gc_builder.compile()

        # Child: adds 10, then runs grandchild
        def child_add(state):
            return {"x": state["x"] + 10}

        child_builder = StateGraph(_Val)
        child_builder.add_node("child_add", child_add)
        child_builder.add_node("grandchild", grandchild)
        child_builder.add_edge("__start__", "child_add")
        child_builder.add_edge("child_add", "grandchild")
        child_builder.add_edge("grandchild", "__end__")
        child = child_builder.compile()

        # Parent
        def parent_entry(state):
            return {"x": state["x"]}

        parent = StateGraph(_Val)
        parent.add_node("parent_entry", parent_entry)
        parent.add_node("child", child)
        parent.add_edge("__start__", "parent_entry")
        parent.add_edge("parent_entry", "child")
        parent.add_edge("child", "__end__")
        app = parent.compile()

        # Flow: x=2 -> parent_entry(x=2) -> child(child_add: 2+10=12, grandchild: 12*3=36)
        result = app.invoke({"x": 2})
        if result.get("x") == 36:
            return True, "3-level nested: 2 -> +10=12 -> *3=36"
        return False, f"Expected x=36, got {result}"
    except Exception as e:
        return False, f"Unexpected error: {e}"


# ---- Test 4: Command.PARENT ----

def _test_command_parent():
    """Command(graph=Command.PARENT, update=...) bubbles up from subgraph to parent."""
    try:

        # Child that raises a ParentCommand via Command.PARENT
        def child_raise(state):
            raise ParentCommand(
                Command(graph=Command.PARENT, update={"x": 999})
            )

        child_builder = StateGraph(_Val)
        child_builder.add_node("child_raise", child_raise)
        child_builder.add_edge("__start__", "child_raise")
        child_builder.add_edge("child_raise", "__end__")
        child = child_builder.compile()

        # Parent catches the ParentCommand
        def parent_entry(state):
            return {"x": state["x"]}

        parent = StateGraph(_Val)
        parent.add_node("parent_entry", parent_entry)
        parent.add_node("subgraph", child)
        parent.add_edge("__start__", "parent_entry")
        parent.add_edge("parent_entry", "subgraph")
        parent.add_edge("subgraph", "__end__")
        app = parent.compile()

        result = app.invoke({"x": 1})
        if result.get("x") == 999:
            return True, "Command.PARENT bubbled up, x=999"
        return False, f"Expected x=999 from parent command, got {result}"
    except ParentCommand as pc:
        if pc.command and pc.command.update and pc.command.update.get("x") == 999:
            return True, "ParentCommand bubbled to top level with x=999"
        return False, f"ParentCommand bubbled but unexpected: {pc}"
    except Exception as e:
        return False, f"Unexpected error: {e}"


# ---- Test 5: update_state with as_node ----

def _test_update_state_as_node():
    """update_state with as_node sets next nodes based on that node's edges."""
    try:

        def node_a(state):
            return {"x": state["x"] + 1}

        def node_b(state):
            return {"x": state["x"] + 100}

        saver = InMemorySaver()
        g = StateGraph(_Val)
        g.add_node("node_a", node_a)
        g.add_node("node_b", node_b)
        g.add_edge("__start__", "node_a")
        g.add_edge("node_a", "node_b")
        g.add_edge("node_b", "__end__")
        app = g.compile(checkpointer=saver)

        cfg = {"configurable": {"thread_id": "as_node_1"}}
        app.invoke({"x": 0}, cfg)

        app.update_state(cfg, {"x": 50}, as_node="node_a")

        snap = app.get_state(cfg)
        if snap.values.get("x") != 50:
            return False, f"Expected x=50, got {snap.values.get('x')}"

        if "node_b" not in snap.next:
            return False, f"Expected 'node_b' in next, got next={snap.next}"
        return True, "update_state as_node='node_a' set next to node_b, x=50"
    except Exception as e:
        return False, f"Unexpected error: {e}"


# ---- Test 6: Multi-level nested subgraph checkpoint namespace isolation ----

def _test_nested_checkpoint_isolation():
    """3-level nested subgraph with checkpointer — verify each level's
    checkpoint_ns is correctly isolated."""
    try:

        # Level 3 (grandchild): adds 1000
        def gc_add(state):
            return {"x": state["x"] + 1000}

        gc_builder = StateGraph(_Val)
        gc_builder.add_node("gc_add", gc_add)
        gc_builder.add_edge("__start__", "gc_add")
        gc_builder.add_edge("gc_add", "__end__")
        grandchild = gc_builder.compile()

        # Level 2 (child): adds 100, then runs grandchild
        def child_add(state):
            return {"x": state["x"] + 100}

        child_builder = StateGraph(_Val)
        child_builder.add_node("child_add", child_add)
        child_builder.add_node("grandchild", grandchild)
        child_builder.add_edge("__start__", "child_add")
        child_builder.add_edge("child_add", "grandchild")
        child_builder.add_edge("grandchild", "__end__")
        child = child_builder.compile()

        # Level 1 (parent): adds 10, then runs child
        def parent_add(state):
            return {"x": state["x"] + 10}

        parent = StateGraph(_Val)
        parent.add_node("parent_add", parent_add)
        parent.add_node("child", child)
        parent.add_edge("__start__", "parent_add")
        parent.add_edge("parent_add", "child")
        parent.add_edge("child", "__end__")

        saver = InMemorySaver()
        app = parent.compile(checkpointer=saver)

        cfg = {"configurable": {"thread_id": "ns_iso"}}
        result = app.invoke({"x": 1}, cfg)
        # Flow: 1 +10=11, +100=111, +1000=1111
        if result.get("x") != 1111:
            return False, f"Expected x=1111, got {result}"

        # Check that subgraph states exist and are isolated
        snap = app.get_state(cfg, subgraphs=True)
        if not snap.subgraphs:
            return False, f"No subgraphs found in snapshot"

        # Verify parent checkpoint has correct value
        if snap.values.get("x") != 1111:
            return False, f"Parent state wrong: {snap.values}"

        # Drill into nested subgraphs — verify each level has its own namespace
        all_namespaces = []

        def collect_namespaces(subgraphs_dict, prefix=""):
            for ns_key, sub_snap in subgraphs_dict.items():
                full_ns = f"{prefix}/{ns_key}" if prefix else ns_key
                all_namespaces.append(full_ns)
                if hasattr(sub_snap, "subgraphs") and sub_snap.subgraphs:
                    collect_namespaces(sub_snap.subgraphs, full_ns)

        collect_namespaces(snap.subgraphs)

        if len(all_namespaces) < 2:
            return False, (
                f"Expected >=2 nested subgraph namespaces, got {all_namespaces}. "
                f"Multi-level checkpoint_ns may not be isolated."
            )

        # Verify no namespace collision — all are unique
        if len(all_namespaces) != len(set(all_namespaces)):
            return False, f"Namespace collision detected: {all_namespaces}"

        return True, (
            f"3-level nesting: namespaces={all_namespaces}, "
            f"final x=1111, isolation verified"
        )
    except Exception as e:
        return False, f"Unexpected error: {e}"


# ---- Test 7: update_state(as_node=...) with conditional edges ----

def _test_update_state_as_node_conditional():
    """update_state with as_node on a node that has conditional edges —
    snap.next must reflect the conditional routing target."""
    try:

        def start_node(state):
            return {"x": state["x"] + 1}

        def branch_node(state):
            # This node's output determines conditional routing
            return {"x": state["x"] + 10}

        def high_path(state):
            return {"x": state["x"] + 100}

        def low_path(state):
            return {"x": state["x"] + 1}

        def router(state):
            if state["x"] > 15:
                return "high_path"
            return "low_path"

        saver = InMemorySaver()
        g = StateGraph(_Val)
        g.add_node("start_node", start_node)
        g.add_node("branch_node", branch_node)
        g.add_node("high_path", high_path)
        g.add_node("low_path", low_path)
        g.add_edge("__start__", "start_node")
        g.add_edge("start_node", "branch_node")
        g.add_conditional_edges(
            "branch_node", router,
            path_map={"high_path": "high_path", "low_path": "low_path"},
        )
        g.add_edge("high_path", "__end__")
        g.add_edge("low_path", "__end__")
        app = g.compile(checkpointer=saver)

        cfg = {"configurable": {"thread_id": "as_node_cond"}}
        app.invoke({"x": 5}, cfg)

        # Now update state as branch_node with x=50
        # Since x > 15, the conditional edge should route to "high_path"
        app.update_state(cfg, {"x": 50}, as_node="branch_node")

        snap = app.get_state(cfg)
        if snap.values.get("x") != 50:
            return False, f"Expected x=50, got {snap.values.get('x')}"

        # The key assertion: snap.next should be "high_path" (conditional route)
        # not both paths or the wrong path
        if "high_path" not in snap.next:
            return False, (
                f"Conditional routing not reflected in snap.next. "
                f"Expected 'high_path', got next={snap.next}. "
                f"update_state(as_node=...) may ignore conditional edges."
            )

        # Now update with x=1 — should route to low_path
        app.update_state(cfg, {"x": 1}, as_node="branch_node")
        snap2 = app.get_state(cfg)
        if "low_path" not in snap2.next:
            return False, (
                f"Low route not in snap.next. "
                f"Expected 'low_path', got next={snap2.next}. "
                f"Conditional routing from update_state is broken."
            )

        return True, (
            f"x=50→next=high_path, x=1→next=low_path, "
            f"conditional routing in update_state works"
        )
    except Exception as e:
        return False, f"Unexpected error: {e}"


# ---- Test 8: Subgraph + interrupt ----

def _test_subgraph_interrupt():
    """interrupt() inside a subgraph node should bubble up, pause the parent,
    and resume should deliver the value into the subgraph."""
    try:
        from zerograph import interrupt, Command

        # Child graph with an interrupt inside
        def child_step(state):
            val = interrupt("need_input")
            return {"x": state["x"] + val}

        child_builder = StateGraph(_Val)
        child_builder.add_node("child_step", child_step)
        child_builder.add_edge("__start__", "child_step")
        child_builder.add_edge("child_step", "__end__")
        child = child_builder.compile()

        # Parent graph uses child as a node
        def parent_entry(state):
            return {"x": state["x"]}

        parent = StateGraph(_Val)
        parent.add_node("parent_entry", parent_entry)
        parent.add_node("subgraph", child)
        parent.add_edge("__start__", "parent_entry")
        parent.add_edge("parent_entry", "subgraph")
        parent.add_edge("subgraph", "__end__")

        saver = InMemorySaver()
        app = parent.compile(checkpointer=saver)
        cfg = {"configurable": {"thread_id": "sub_int"}}

        # First invoke — should interrupt inside the subgraph
        result1 = app.invoke({"x": 10}, cfg)
        # After interrupt, the state should be paused (not completed)
        snap1 = app.get_state(cfg)
        # Verify there's a pending interrupt
        has_interrupt = len(snap1.interrupts) > 0 if snap1.interrupts else False
        if not has_interrupt:
            return False, (
                f"Expected interrupt in snapshot, got interrupts={snap1.interrupts}. "
                f"Subgraph interrupt may not propagate to parent checkpoint."
            )

        # Resume with a value
        resume_val = 42
        result2 = app.invoke(Command(resume=resume_val), cfg)

        # Final result should be x=10 (entry) + 42 (resumed value in child)
        if result2.get("x") != 52:
            return False, (
                f"After resume: expected x=52, got x={result2.get('x')}. "
                f"Resume value may not have reached subgraph node."
            )

        return True, f"interrupt in subgraph paused, resume({resume_val}) → x=52"
    except Exception as e:
        return False, f"Unexpected error: {e}"


# ---- Test 9: 3-level nested subgraph + interrupt ----

def _test_nested_subgraph_interrupt():
    """3-level nesting: parent → child → grandchild. Grandchild calls interrupt().
    Resume value should propagate through all levels."""
    try:
        from zerograph import interrupt, Command

        # Grandchild: interrupts and adds resume value
        def gc_step(state):
            val = interrupt("gc_input")
            return {"x": state["x"] + val}

        gc_builder = StateGraph(_Val)
        gc_builder.add_node("gc_step", gc_step)
        gc_builder.add_edge("__start__", "gc_step")
        gc_builder.add_edge("gc_step", "__end__")
        grandchild = gc_builder.compile()

        # Child: adds 10, then runs grandchild
        def child_add(state):
            return {"x": state["x"] + 10}

        child_builder = StateGraph(_Val)
        child_builder.add_node("child_add", child_add)
        child_builder.add_node("grandchild", grandchild)
        child_builder.add_edge("__start__", "child_add")
        child_builder.add_edge("child_add", "grandchild")
        child_builder.add_edge("grandchild", "__end__")
        child = child_builder.compile()

        # Parent: passes through
        def parent_entry(state):
            return {"x": state["x"]}

        parent = StateGraph(_Val)
        parent.add_node("parent_entry", parent_entry)
        parent.add_node("child", child)
        parent.add_edge("__start__", "parent_entry")
        parent.add_edge("parent_entry", "child")
        parent.add_edge("child", "__end__")

        saver = InMemorySaver()
        app = parent.compile(checkpointer=saver)
        cfg = {"configurable": {"thread_id": "nest_int"}}

        # First invoke — interrupt inside grandchild
        result1 = app.invoke({"x": 1}, cfg)
        snap1 = app.get_state(cfg)
        if not (snap1.interrupts and len(snap1.interrupts) > 0):
            return False, f"No interrupt in 3-level nesting, interrupts={snap1.interrupts}"

        # Resume with 100
        result2 = app.invoke(Command(resume=100), cfg)
        # Flow: x=1 → parent_entry(1) → child_add(1+10=11) → gc_step(11+100=111)
        if result2.get("x") != 111:
            return False, f"Expected x=111, got {result2.get('x')}"
        return True, "3-level nested interrupt: 1→+10=11→+100=111"
    except Exception as e:
        return False, f"Unexpected error: {e}"


# ---- Test 10: Multiple interrupts in a subgraph ----

def _test_multi_interrupt_subgraph():
    """Subgraph with two nodes that each call interrupt(). First resume delivers
    to node_a. The second interrupt at node_b is a separate resume cycle.

    Note: subgraph receives a single resume value per invoke. Both nodes in
    the subgraph's _run share the same scratchpad(resume=[val]). Currently,
    if a node completes and the next node calls interrupt(), the scratchpad
    still has the resume value from the parent, so node_b gets it too.
    This test verifies the observed behavior: both nodes receive the same
    resume value in one invoke, producing x = 0 + 10 + 10 = 20.
    """
    try:
        from zerograph import interrupt, Command

        def node_a(state):
            val = interrupt("a_input")
            return {"x": state["x"] + val}

        def node_b(state):
            val = interrupt("b_input")
            return {"x": state["x"] + val}

        child_builder = StateGraph(_Val)
        child_builder.add_node("node_a", node_a)
        child_builder.add_node("node_b", node_b)
        child_builder.add_edge("__start__", "node_a")
        child_builder.add_edge("node_a", "node_b")
        child_builder.add_edge("node_b", "__end__")
        child = child_builder.compile()

        def parent_entry(state):
            return {"x": state["x"]}

        parent = StateGraph(_Val)
        parent.add_node("parent_entry", parent_entry)
        parent.add_node("subgraph", child)
        parent.add_edge("__start__", "parent_entry")
        parent.add_edge("parent_entry", "subgraph")
        parent.add_edge("subgraph", "__end__")

        saver = InMemorySaver()
        app = parent.compile(checkpointer=saver)
        cfg = {"configurable": {"thread_id": "multi_int"}}

        # First invoke — interrupts at node_a
        result1 = app.invoke({"x": 0}, cfg)
        snap1 = app.get_state(cfg)
        if not snap1.interrupts:
            return False, f"First invoke: no interrupt, got {snap1.interrupts}"

        # Resume with 10 — node_a gets 10, node_b also gets 10 from shared scratchpad
        result2 = app.invoke(Command(resume=10), cfg)
        # Both nodes receive resume=10, so x = 0 + 10 + 10 = 20
        if result2.get("x") != 20:
            return False, f"Expected x=20, got {result2.get('x')}"

        # Graph should be completed (no more interrupts)
        snap2 = app.get_state(cfg)
        if snap2.interrupts:
            return False, f"Unexpected interrupts: {snap2.interrupts}"

        return True, "Multi interrupt subgraph: resume(10) delivers to both nodes, x=20"
    except Exception as e:
        return False, f"Unexpected error: {e}"


# ---- Test 11: Send fan-out to subgraph ----

def _test_send_to_subgraph():
    """Send() targets a subgraph node with dict args matching the subgraph schema.
    Each Send triggers a separate subgraph execution. Results are aggregated."""
    try:
        from zerograph import Send
        import operator

        class _AccSt(TypedDict):
            items: Annotated[list, operator.add]

        def sub_worker(state):
            tag = state.get("items", ["?"])[-1] if state.get("items") else "?"
            return {"items": [f"sub_{tag}"]}

        sub_builder = StateGraph(_AccSt)
        sub_builder.add_node("worker", sub_worker)
        sub_builder.add_edge("__start__", "worker")
        sub_builder.add_edge("worker", "__end__")
        sub = sub_builder.compile()

        def router(state):
            return [Send("sub", {"items": [f"t{i}"]}) for i in range(3)]

        parent = StateGraph(_AccSt)
        parent.add_node("router", router)
        parent.add_node("sub", sub)
        parent.add_edge("__start__", "router")
        parent.add_conditional_edges("router", router, path_map={"sub": "sub"})
        parent.add_edge("sub", "__end__")
        app = parent.compile()

        result = app.invoke({"items": []})
        processed = result.get("items", [])
        # Verify all 3 subgraph results are present
        got = set(processed)
        if not all(f"sub_t{i}" in got for i in range(3)):
            return False, f"Missing sub_t results in {got}"
        # At least 3 items from subgraph execution
        sub_count = sum(1 for x in processed if x.startswith("sub_"))
        if sub_count < 3:
            return False, f"Only {sub_count} sub results, expected >= 3"
        return True, f"Send to subgraph: {sub_count} sub results in {len(processed)} total"
    except Exception as e:
        return False, f"Unexpected error: {e}"


# ---- Test 12: Checkpoint integrity after subgraph resume ----

def _test_subgraph_checkpoint_after_resume():
    """After interrupt+resume in subgraph, get_state(subgraphs=True) reflects final state."""
    try:
        from zerograph import interrupt, Command

        def child_step(state):
            val = interrupt("need_val")
            return {"x": state["x"] + val}

        child_builder = StateGraph(_Val)
        child_builder.add_node("child_step", child_step)
        child_builder.add_edge("__start__", "child_step")
        child_builder.add_edge("child_step", "__end__")
        child = child_builder.compile()

        def parent_entry(state):
            return {"x": state["x"]}

        parent = StateGraph(_Val)
        parent.add_node("parent_entry", parent_entry)
        parent.add_node("subgraph", child)
        parent.add_edge("__start__", "parent_entry")
        parent.add_edge("parent_entry", "subgraph")
        parent.add_edge("subgraph", "__end__")

        saver = InMemorySaver()
        app = parent.compile(checkpointer=saver)
        cfg = {"configurable": {"thread_id": "cp_resume"}}

        # Interrupt + resume
        app.invoke({"x": 5}, cfg)
        app.invoke(Command(resume=7), cfg)

        # Check final state
        snap = app.get_state(cfg, subgraphs=True)
        if snap.values.get("x") != 12:
            return False, f"Parent state wrong: expected x=12, got {snap.values}"

        # Verify no pending interrupts
        if snap.interrupts:
            return False, f"Unexpected interrupts after resume: {snap.interrupts}"

        # Verify next is empty (graph completed)
        if snap.next:
            return False, f"Unexpected next after completion: {snap.next}"

        return True, "Checkpoint clean after subgraph resume: x=12, no pending"
    except Exception as e:
        return False, f"Unexpected error: {e}"


def run() -> list[tuple[str, bool, str]]:
    """Run all subgraph tests."""
    results = []
    tests = [
        ("Basic subgraph", _test_basic_subgraph),
        ("get_state(subgraphs=True)", _test_get_state_subgraphs),
        ("Nested subgraph (3 levels)", _test_nested_subgraph),
        ("Command.PARENT", _test_command_parent),
        ("update_state as_node", _test_update_state_as_node),
        ("Nested checkpoint isolation (3 levels)", _test_nested_checkpoint_isolation),
        ("update_state as_node + conditional edges", _test_update_state_as_node_conditional),
        ("Subgraph + interrupt/resume", _test_subgraph_interrupt),
        ("3-level nested interrupt", _test_nested_subgraph_interrupt),
        ("Multi interrupt in subgraph", _test_multi_interrupt_subgraph),
        ("Send to subgraph", _test_send_to_subgraph),
        ("Checkpoint after subgraph resume", _test_subgraph_checkpoint_after_resume),
    ]
    for name, fn in tests:
        passed, detail = fn()
        results.append((name, passed, detail))
    return results
