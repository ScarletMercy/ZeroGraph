"""s01_basic — Basic graph construction tests."""

from typing import TypedDict

from zerograph import StateGraph, START, END


class St(TypedDict):
    value: int


# ---------------------------------------------------------------------------
# Test 1: Linear graph  START -> inc -> double -> END
# invoke({"value": 3}) -> {"value": 8}
# ---------------------------------------------------------------------------

def _test_linear() -> tuple[str, bool, str]:
    def inc(state: St) -> dict:
        return {"value": state["value"] + 1}

    def double(state: St) -> dict:
        return {"value": state["value"] * 2}

    g = StateGraph(St)
    g.add_node("inc", inc)
    g.add_node("double", double)
    g.add_edge(START, "inc")
    g.add_edge("inc", "double")
    g.add_edge("double", END)
    compiled = g.compile()
    result = compiled.invoke({"value": 3})
    if result == {"value": 8}:
        return ("linear graph", True, "3 -> inc(4) -> double(8)")
    return ("linear graph", False, f"expected {{'value': 8}}, got {result}")


# ---------------------------------------------------------------------------
# Test 2: Conditional edges — route based on value > 5
# ---------------------------------------------------------------------------

def _test_conditional() -> tuple[str, bool, str]:
    def inc(state: St) -> dict:
        return {"value": state["value"] + 1}

    def big(state: St) -> dict:
        return {"value": state["value"] * 10}

    def small(state: St) -> dict:
        return {"value": state["value"] + 1}

    def router(state: St) -> str:
        return "big" if state["value"] > 5 else "small"

    g = StateGraph(St)
    g.add_node("inc", inc)
    g.add_node("big", big)
    g.add_node("small", small)
    g.add_edge(START, "inc")
    g.add_conditional_edges("inc", router, {"big": "big", "small": "small"})
    g.add_edge("big", END)
    g.add_edge("small", END)
    compiled = g.compile()

    r1 = compiled.invoke({"value": 10})  # 10+1=11 > 5 -> big -> 110
    r2 = compiled.invoke({"value": 2})   # 2+1=3 <= 5 -> small -> 4
    if r1["value"] == 110 and r2["value"] == 4:
        return ("conditional edges", True, "routed big=110, small=4")
    return ("conditional edges", False, f"got {r1}, {r2}")


# ---------------------------------------------------------------------------
# Test 3: add_sequence — 3 nodes in sequence
# ---------------------------------------------------------------------------

def _test_sequence() -> tuple[str, bool, str]:
    def step_a(state: St) -> dict:
        return {"value": state["value"] + 1}

    def step_b(state: St) -> dict:
        return {"value": state["value"] * 2}

    def step_c(state: St) -> dict:
        return {"value": state["value"] + 10}

    g = StateGraph(St)
    g.add_sequence([step_a, step_b, step_c])
    g.add_edge(START, "step_a")
    g.add_edge("step_c", END)
    compiled = g.compile()
    result = compiled.invoke({"value": 3})  # 3+1=4 -> 4*2=8 -> 8+10=18
    if result["value"] == 18:
        return ("add_sequence", True, "3 nodes: +1, *2, +10 => 18")
    return ("add_sequence", False, f"expected 18, got {result['value']}")


# ---------------------------------------------------------------------------
# Test 4: Graph validation catches missing node
# ---------------------------------------------------------------------------

def _test_validation() -> tuple[str, bool, str]:
    # Test: graph with no START edge should fail validation
    g = StateGraph(St)
    g.add_node("a", lambda s: s)
    # No edge from START added
    try:
        g.validate()
        return ("validation", False, "expected ValueError: no START edge")
    except ValueError as e:
        if "entrypoint" in str(e).lower() or "START" in str(e) or "start" in str(e).lower():
            return ("validation", True, f"caught missing entrypoint: {e}")
        return ("validation", False, f"wrong error: {e}")


# ---------------------------------------------------------------------------
# Runner
# ---------------------------------------------------------------------------

def run() -> list[tuple[str, bool, str]]:
    results: list[tuple[str, bool, str]] = []
    for fn in (_test_linear, _test_conditional, _test_sequence, _test_validation):
        try:
            results.append(fn())
        except Exception as exc:
            results.append((fn.__name__, False, str(exc)))
    return results
