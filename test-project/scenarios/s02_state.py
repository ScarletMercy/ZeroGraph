"""s02_state — State management tests."""

from typing import Annotated, TypedDict

from zerograph import StateGraph, START, END, add_messages, RemoveMessage, StateSnapshot


class AccSt(TypedDict):
    total: Annotated[int, lambda old, new: old + new]


class MsgSt(TypedDict):
    messages: Annotated[list, add_messages]


# ---------------------------------------------------------------------------
# Test 1: Custom reducer — accumulator across invocations (with checkpointer)
# ---------------------------------------------------------------------------

def _test_custom_reducer() -> tuple[str, bool, str]:
    from zerograph import InMemorySaver

    def add_val(state: AccSt) -> dict:
        return {"total": 5}

    saver = InMemorySaver()
    g = StateGraph(AccSt)
    g.add_node("add_val", add_val)
    g.add_edge(START, "add_val")
    g.add_edge("add_val", END)
    compiled = g.compile(checkpointer=saver)

    cfg = {"configurable": {"thread_id": "t1"}}
    r1 = compiled.invoke({"total": 10}, cfg)   # 10 + 5 = 15
    r2 = compiled.invoke({"total": 3}, cfg)    # reducer: 15+3=18, then +5=23
    if r1["total"] == 15 and r2["total"] == 23:
        return ("custom reducer", True, "10+5=15, 15+3+5=23")
    return ("custom reducer", False, f"got r1={r1}, r2={r2}")


# ---------------------------------------------------------------------------
# Test 2: add_messages — add, update by id, verify merge
# ---------------------------------------------------------------------------

def _test_add_messages() -> tuple[str, bool, str]:
    def add_msg(state: MsgSt) -> dict:
        return {"messages": [{"id": "m1", "text": "hello"}]}

    def update_msg(state: MsgSt) -> dict:
        return {"messages": [{"id": "m1", "text": "updated"}]}

    g = StateGraph(MsgSt)
    g.add_node("add_msg", add_msg)
    g.add_node("update_msg", update_msg)
    g.add_edge(START, "add_msg")
    g.add_edge("add_msg", "update_msg")
    g.add_edge("update_msg", END)
    compiled = g.compile()
    result = compiled.invoke({"messages": []})
    msgs = result["messages"]
    if len(msgs) == 1 and msgs[0]["text"] == "updated" and msgs[0]["id"] == "m1":
        return ("add_messages", True, "added then updated by id")
    return ("add_messages", False, f"got {msgs}")


# ---------------------------------------------------------------------------
# Test 3: RemoveMessage — add then remove a message
# ---------------------------------------------------------------------------

def _test_remove_message() -> tuple[str, bool, str]:
    def add_msgs(state: MsgSt) -> dict:
        return {"messages": [
            {"id": "a", "text": "first"},
            {"id": "b", "text": "second"},
        ]}

    def remove_one(state: MsgSt) -> dict:
        return {"messages": [RemoveMessage(id="a")]}

    g = StateGraph(MsgSt)
    g.add_node("add_msgs", add_msgs)
    g.add_node("remove_one", remove_one)
    g.add_edge(START, "add_msgs")
    g.add_edge("add_msgs", "remove_one")
    g.add_edge("remove_one", END)
    compiled = g.compile()
    result = compiled.invoke({"messages": []})
    msgs = result["messages"]
    if len(msgs) == 1 and msgs[0]["id"] == "b":
        return ("RemoveMessage", True, "removed msg 'a', kept 'b'")
    return ("RemoveMessage", False, f"got {msgs}")


# ---------------------------------------------------------------------------
# Test 4: update_state — manual update, next execution sees it
# ---------------------------------------------------------------------------

def _test_update_state() -> tuple[str, bool, str]:
    from zerograph import InMemorySaver

    class SimpleSt(TypedDict):
        value: int

    def inc(state: SimpleSt) -> dict:
        return {"value": state["value"] + 1}

    saver = InMemorySaver()
    g = StateGraph(SimpleSt)
    g.add_node("inc", inc)
    g.add_edge(START, "inc")
    g.add_edge("inc", END)
    compiled = g.compile(checkpointer=saver)

    cfg = {"configurable": {"thread_id": "t4"}}
    r1 = compiled.invoke({"value": 0}, cfg)      # 0+1=1
    compiled.update_state(cfg, {"value": 99})     # manually set to 99
    snap = compiled.get_state(cfg)
    if snap.values.get("value") != 99:
        return ("update_state", False, f"expected 99, got {snap.values}")
    # Verify next execution sees the updated value
    r2 = compiled.invoke(None, cfg)               # should apply 99+1=100
    if r2["value"] != 100:
        return ("update_state", False, f"after resume: expected 100, got {r2}")
    return ("update_state", True, "updated to 99, next invoke sees 99+1=100")


# ---------------------------------------------------------------------------
# Test 5: get_state — verify StateSnapshot fields
# ---------------------------------------------------------------------------

def _test_get_state() -> tuple[str, bool, str]:
    from zerograph import InMemorySaver

    class SimpleSt(TypedDict):
        value: int

    def inc(state: SimpleSt) -> dict:
        return {"value": state["value"] + 1}

    saver = InMemorySaver()
    g = StateGraph(SimpleSt)
    g.add_node("inc", inc)
    g.add_edge(START, "inc")
    g.add_edge("inc", END)
    compiled = g.compile(checkpointer=saver)

    cfg = {"configurable": {"thread_id": "t5_getstate"}}
    compiled.invoke({"value": 5}, cfg)
    snap: StateSnapshot = compiled.get_state(cfg)

    if not isinstance(snap, StateSnapshot):
        return ("get_state", False, f"not StateSnapshot: {type(snap)}")
    if not isinstance(snap.values, dict):
        return ("get_state", False, f"values not dict: {type(snap.values)}")
    if snap.values.get("value") != 6:
        return ("get_state", False, f"expected value=6, got {snap.values}")
    if not isinstance(snap.next, tuple):
        return ("get_state", False, f"next not tuple: {type(snap.next)}")
    if snap.config is None:
        return ("get_state", False, "config is None")
    return ("get_state", True, f"values={snap.values}, next={snap.next}")


# ---------------------------------------------------------------------------
# Runner
# ---------------------------------------------------------------------------

def run() -> list[tuple[str, bool, str]]:
    results: list[tuple[str, bool, str]] = []
    for fn in (
        _test_custom_reducer,
        _test_add_messages,
        _test_remove_message,
        _test_update_state,
        _test_get_state,
    ):
        try:
            results.append(fn())
        except Exception as exc:
            results.append((fn.__name__, False, str(exc)))
    return results
