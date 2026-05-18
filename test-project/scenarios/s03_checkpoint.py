"""s03_checkpoint — Checkpointing tests."""

import asyncio
from typing import TypedDict

from zerograph import (
    StateGraph,
    START,
    END,
    InMemorySaver,
    SqliteSaver,
    AsyncSqliteSaver,
    Checkpoint,
    CheckpointMetadata,
)


class St(TypedDict):
    value: int


def _inc(state: St) -> dict:
    return {"value": state["value"] + 1}


def _build_graph(checkpointer=None):
    g = StateGraph(St)
    g.add_node("inc", _inc)
    g.add_edge(START, "inc")
    g.add_edge("inc", END)
    return g.compile(checkpointer=checkpointer)


# ---------------------------------------------------------------------------
# Test 1: InMemorySaver — invoke twice, checkpoint persists
# ---------------------------------------------------------------------------

def _test_in_memory() -> tuple[str, bool, str]:
    saver = InMemorySaver()
    compiled = _build_graph(saver)
    cfg = {"configurable": {"thread_id": "mem1"}}
    compiled.invoke({"value": 42}, cfg)

    tup = saver.get_tuple(cfg)
    if tup is not None and tup.checkpoint is not None:
        cv = tup.checkpoint.get("channel_values", {})
        if cv.get("value") == 43:
            return ("InMemorySaver basic", True, "checkpoint saved with value=43")
        return ("InMemorySaver basic", False, f"expected 43, got {cv}")
    return ("InMemorySaver basic", False, "no checkpoint found")


# ---------------------------------------------------------------------------
# Test 2: SqliteSaver — same test with SQLite
# ---------------------------------------------------------------------------

def _test_sqlite() -> tuple[str, bool, str]:
    with SqliteSaver(":memory:") as saver:
        compiled = _build_graph(saver)
        cfg = {"configurable": {"thread_id": "sq1"}}
        compiled.invoke({"value": 42}, cfg)

        tup = saver.get_tuple(cfg)
        if tup is not None and tup.checkpoint is not None:
            cv = tup.checkpoint.get("channel_values", {})
            if cv.get("value") == 43:
                return ("SqliteSaver", True, "saved value=43 after inc(42)")
            return ("SqliteSaver", False, f"expected 43, got {cv}")
        return ("SqliteSaver", False, "no checkpoint found")


# ---------------------------------------------------------------------------
# Test 3: AsyncSqliteSaver — aget_tuple / aput
# ---------------------------------------------------------------------------

def _test_async_sqlite() -> tuple[str, bool, str]:
    async def _inner():
        saver = AsyncSqliteSaver(":memory:")

        # Sub-test A: aput + aget_tuple round-trip (tests the async path directly)
        cp: Checkpoint = {
            "v": 1,
            "id": "test-cp-1",
            "ts": "2025-01-01T00:00:00",
            "channel_values": {"value": 11},
        }
        meta: CheckpointMetadata = {"source": "test", "step": 0}
        cfg1 = {"configurable": {"thread_id": "asq1"}}
        new_cfg1 = await saver.aput(cfg1, cp, meta)
        tup1 = await saver.aget_tuple(new_cfg1)
        if tup1 is None:
            return ("AsyncSqliteSaver", False, "aput + aget_tuple returned None")
        cv1 = tup1.checkpoint.get("channel_values", {})
        if cv1.get("value") != 11:
            return ("AsyncSqliteSaver", False, f"expected 11, got {cv1}")

        # Sub-test B: second aput + aget_tuple round-trip
        cp2: Checkpoint = {
            "v": 1,
            "id": "test-cp-2",
            "ts": "2025-01-01T00:00:00",
            "channel_values": {"value": 999},
        }
        cfg2 = {"configurable": {"thread_id": "asq2"}}
        new_cfg2 = await saver.aput(cfg2, cp2, meta)
        tup2 = await saver.aget_tuple(new_cfg2)
        if tup2 is None:
            return ("AsyncSqliteSaver", False, "aput + aget_tuple (2nd) returned None")
        cv2 = tup2.checkpoint.get("channel_values", {})
        if cv2.get("value") != 999:
            return ("AsyncSqliteSaver", False, f"aput round-trip: expected 999, got {cv2}")

        return ("AsyncSqliteSaver", True, "aput/aget_tuple: 11 and 999 verified")

    try:
        return asyncio.run(_inner())
    except Exception as exc:
        return ("AsyncSqliteSaver", False, str(exc))


# ---------------------------------------------------------------------------
# Test 4: State history — invoke 3 times, get_state_history returns 3+ snapshots
# ---------------------------------------------------------------------------

def _test_state_history() -> tuple[str, bool, str]:
    saver = InMemorySaver()
    compiled = _build_graph(saver)
    cfg = {"configurable": {"thread_id": "hist1"}}
    compiled.invoke({"value": 1}, cfg)
    compiled.invoke({"value": 2}, cfg)
    compiled.invoke({"value": 3}, cfg)

    history = compiled.get_state_history(cfg, limit=10)
    if len(history) < 3:
        return ("state history", False, f"expected >=3, got {len(history)}")

    # Verify snapshots contain actual state values
    values = [snap.values.get("value") for snap in history]
    # After 3 invocations with input values 1,2,3 the final values should be 2,3,4
    found = set(v for v in values if v is not None)
    if len(found) < 3:
        return ("state history", False, f"expected distinct values, got {values}")
    return ("state history", True, f"{len(history)} snapshots, values={sorted(found)}")


# ---------------------------------------------------------------------------
# Test 5: Parent chain — at least one snapshot has non-None parent_config
# ---------------------------------------------------------------------------

def _test_parent_chain() -> tuple[str, bool, str]:
    with SqliteSaver(":memory:") as saver:
        compiled = _build_graph(saver)
        cfg = {"configurable": {"thread_id": "parent1"}}
        compiled.invoke({"value": 1}, cfg)
        compiled.invoke({"value": 2}, cfg)

        history = compiled.get_state_history(cfg, limit=10)
        parents = [snap for snap in history if snap.parent_config is not None]
        if not parents:
            return ("parent chain", False, f"no parent_config in {len(history)} snapshots")

        # Verify parent chain is traversable: follow first parent to its checkpoint
        first_parent = parents[0]
        parent_cfg = first_parent.parent_config
        parent_snap = saver.get_tuple(parent_cfg)
        if parent_snap is None:
            return ("parent chain", False, "parent_config points to non-existent checkpoint")
        return ("parent chain", True,
                f"{len(parents)}/{len(history)} have parent, chain traversable")


# ---------------------------------------------------------------------------
# Test 6: SqliteSaver context manager
# ---------------------------------------------------------------------------

def _test_sqlite_context_mgr() -> tuple[str, bool, str]:
    try:
        with SqliteSaver(":memory:") as saver:
            compiled = _build_graph(saver)
            cfg = {"configurable": {"thread_id": "ctx1"}}
            r = compiled.invoke({"value": 7}, cfg)
            if r["value"] == 8:
                return (
                    "SqliteSaver context manager",
                    True,
                    "with-statement works, result=8",
                )
            return ("SqliteSaver context manager", False, f"expected 8, got {r}")
    except Exception as exc:
        return ("SqliteSaver context manager", False, str(exc))


# ---------------------------------------------------------------------------
# Runner
# ---------------------------------------------------------------------------

def run() -> list[tuple[str, bool, str]]:
    results: list[tuple[str, bool, str]] = []
    for fn in (
        _test_in_memory,
        _test_sqlite,
        _test_async_sqlite,
        _test_state_history,
        _test_parent_chain,
        _test_sqlite_context_mgr,
    ):
        try:
            results.append(fn())
        except Exception as exc:
            results.append((fn.__name__, False, str(exc)))
    return results
