"""s07_streaming -- All stream modes."""

from typing import TypedDict

from zerograph import (
    START,
    END,
    StateGraph,
    InMemorySaver,
)


class St(TypedDict):
    x: int


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _inc(state: St) -> dict:
    return {"x": state["x"] + 1}


def _double(state: St) -> dict:
    return {"x": state["x"] * 2}


def _two_step_graph():
    """START -> inc -> double -> END"""
    g = StateGraph(St)
    g.add_node("inc", _inc)
    g.add_node("double", _double)
    g.add_edge(START, "inc")
    g.add_edge("inc", "double")
    g.add_edge("double", END)
    return g.compile()


def _gen_graph():
    """Graph with a generator node for messages mode."""
    def gen_node(state: St):
        yield {"x": 1}
        yield {"x": 2}
        return {"x": 3}

    g = StateGraph(St)
    g.add_node("gen", gen_node)
    g.add_edge(START, "gen")
    g.add_edge("gen", END)
    return g.compile()


def _custom_graph():
    """Graph whose node uses the custom writer."""
    def writer_node(state: St, config):
        w = config["configurable"].get("__writer__")
        if w:
            w("data")
        return {"x": state["x"] + 1}

    g = StateGraph(St)
    g.add_node("w", writer_node)
    g.add_edge(START, "w")
    g.add_edge("w", END)
    return g.compile()


def _checkpoint_graph():
    """Two-step graph compiled with an InMemorySaver for checkpoint mode."""
    g = StateGraph(St)
    g.add_node("inc", _inc)
    g.add_node("double", _double)
    g.add_edge(START, "inc")
    g.add_edge("inc", "double")
    g.add_edge("double", END)
    return g.compile(checkpointer=InMemorySaver())


# ---------------------------------------------------------------------------
# run()
# ---------------------------------------------------------------------------

def run() -> list[tuple[str, bool, str]]:
    results: list[tuple[str, bool, str]] = []

    # 1. "values" mode -------------------------------------------------
    try:
        graph = _two_step_graph()
        events = list(graph.stream({"x": 1}, stream_mode="values"))
        ok = len(events) == 2 and events[-1] == {"x": 4}
        results.append((
            "values mode: 2-step graph yields 2 value events",
            ok,
            f"got {len(events)} events, last={events[-1] if events else None}",
        ))
    except Exception as exc:
        results.append(("values mode: 2-step graph yields 2 value events", False, str(exc)))

    # 2. "updates" mode ------------------------------------------------
    try:
        graph = _two_step_graph()
        events = list(graph.stream({"x": 1}, stream_mode="updates"))
        if len(events) != 2:
            results.append((
                "updates mode: 2 nodes yield 2 update events",
                False, f"expected 2 events, got {len(events)}",
            ))
        else:
            # Verify content: first event should have inc's output, second should have double's
            has_inc = any(
                isinstance(e, dict) and "inc" in e and e["inc"].get("x") == 2
                for e in events
            )
            has_double = any(
                isinstance(e, dict) and "double" in e and e["double"].get("x") == 4
                for e in events
            )
            ok = has_inc and has_double
            results.append((
                "updates mode: 2 nodes yield correct update data",
                ok,
                f"inc={has_inc}, double={has_double}, events={events}",
            ))
    except Exception as exc:
        results.append(("updates mode: 2 nodes yield 2 update events", False, str(exc)))

    # 3. "messages" mode -----------------------------------------------
    try:
        graph = _gen_graph()
        events = list(graph.stream({"x": 0}, stream_mode="messages"))
        if len(events) != 2:
            results.append((
                "messages mode: generator node yields 2 chunks",
                False, f"expected 2 events, got {len(events)}",
            ))
        else:
            chunks = [e.get("chunk") for e in events if isinstance(e, dict)]
            expected_chunks = [{"x": 1}, {"x": 2}]
            ok = chunks == expected_chunks
            results.append((
                "messages mode: generator node yields correct chunks",
                ok,
                f"chunks={chunks}",
            ))
    except Exception as exc:
        results.append(("messages mode: generator node yields 2 chunks", False, str(exc)))

    # 4. "custom" mode -------------------------------------------------
    try:
        graph = _custom_graph()
        events = list(graph.stream({"x": 0}, stream_mode="custom"))
        ok = (
            len(events) == 1
            and events[0].get("value") == "data"
        )
        results.append((
            "custom mode: node calls writer, custom event received",
            ok,
            f"got {len(events)} events, first={events[0] if events else None}",
        ))
    except Exception as exc:
        results.append(("custom mode: node calls writer, custom event received", False, str(exc)))

    # 5. "debug" mode --------------------------------------------------
    try:
        graph = _two_step_graph()
        events = list(graph.stream({"x": 1}, stream_mode="debug"))
        if not events:
            results.append(("debug mode: events present and have type field", False, "no events"))
        else:
            has_type = all(isinstance(e, dict) and "type" in e for e in events)
            # Accept actual debug event types: task, task_result, step_end, etc.
            results.append((
                "debug mode: events have valid type field",
                has_type,
                f"{len(events)} events, types={[e.get('type') for e in events]}",
            ))
    except Exception as exc:
        results.append(("debug mode: events have 'type' field", False, str(exc)))

    # 6. "checkpoints" mode --------------------------------------------
    try:
        graph = _checkpoint_graph()
        config = {"configurable": {"thread_id": "cp-test"}}
        events = list(graph.stream({"x": 1}, config, stream_mode="checkpoints"))
        has_cp_id = all(
            isinstance(e, dict) and "checkpoint_id" in e for e in events
        )
        results.append((
            "checkpoints mode: events have checkpoint_id",
            has_cp_id and len(events) >= 1,
            f"got {len(events)} events, has_checkpoint_id={has_cp_id}",
        ))
    except Exception as exc:
        results.append(("checkpoints mode: events have checkpoint_id", False, str(exc)))

    # 7. "tasks" mode --------------------------------------------------
    try:
        graph = _two_step_graph()
        events = list(graph.stream({"x": 1}, stream_mode="tasks"))
        types = [e.get("type") for e in events]
        has_start = "task_start" in types
        has_end = "task_end" in types
        results.append((
            "tasks mode: task_start/task_end events present",
            has_start and has_end,
            f"types={types}",
        ))
    except Exception as exc:
        results.append(("tasks mode: task_start/task_end events present", False, str(exc)))

    # 8. Multi-mode ----------------------------------------------------
    try:
        graph = _two_step_graph()
        events = list(graph.stream(
            {"x": 1},
            stream_mode=["values", "updates", "tasks"],
        ))
        all_tuples = all(isinstance(e, tuple) and len(e) == 2 for e in events)
        modes_seen = {e[0] for e in events} if all_tuples else set()
        results.append((
            "multi-mode: events are (mode, data) tuples",
            all_tuples and modes_seen.issuperset({"values", "updates", "tasks"}),
            f"modes_seen={modes_seen}",
        ))
    except Exception as exc:
        results.append(("multi-mode: events are (mode, data) tuples", False, str(exc)))

    return results
