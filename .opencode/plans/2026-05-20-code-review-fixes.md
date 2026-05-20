# ZeroGraph Code Review Fixes Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Fix 8 confirmed bugs and code quality issues across the ZeroGraph codebase.

**Architecture:** Pure Python graph execution engine. Fixes target checkpoint serialization, channel system, prebuilt agents, and project metadata. No new dependencies.

**Tech Stack:** Python 3.11+, pytest, pytest-asyncio

**Verification Results:** 30 originally reported issues were tested. 12 were confirmed (some merged as duplicates), 10 were false positives, 8 are code style preferences.

### Confirmed Bugs (to fix)
| # | Severity | Issue | Status |
|---|----------|-------|--------|
| 3 | Critical | `_safe_copy` shallow copy | CONFIRMED |
| 4 | Critical | SQLite JSON `default=str` loses types | CONFIRMED |
| 9 | Medium | `create_supervisor` variable order (fragile) | CONFIRMED |
| 12 | Low | `LLMStreamAdapter` sparse indices | CONFIRMED |
| 13 | Minor | `TAG_HIDDEN` langsmith coupling | CONFIRMED |
| 14 | Minor | Missing `__all__` exports | CONFIRMED |
| 15 | Minor | Missing pyproject.toml URLs | CONFIRMED |

### False Positives (NOT bugs)
| # | Issue | Reason |
|---|-------|--------|
| 1 | `asyncio.run` bug | `pool.submit(asyncio.run, coro())` is valid Python |
| 2 | Parallel race condition | Result processing is sequential, no race |
| 5 | Swarm closure | Late binding works correctly |
| 7 | `__slots__` missing typ | `BaseChannel.__slots__` already declares `typ` |
| 8 | Type init edge cases | Handles int/str/list/typing.List correctly |
| 11 | `Topic._flatten` | Design choice, not a bug |
| 16 | func.py stream limitation | Documented behavior, not a bug |
| 18 | InMemorySaver pending_writes | New list each time, no reference issue |

### Code Style (deferred)
| # | Issue | Reason |
|---|-------|--------|
| 6 | Error context `str(e)` | Minor improvement, not critical |
| 10 | Imports in functions | Python caches imports, negligible overhead |

---

## Phase 1: Critical Bugs (data integrity)

### Task 1: Fix `_safe_copy` shallow copy issue

**Files:**
- Modify: `zerograph/channels/binop.py:22-28`
- Test: `tests/test_advanced.py`

**Problem:** `_safe_copy` uses `.copy()` which is a shallow copy. When checkpoint values contain nested mutable objects, modifications to the restored checkpoint will affect the original.

**Step 1: Write test for deep copy in checkpoint**

Add to `tests/test_advanced.py`:

```python
def test_checkpoint_deep_copy_nested():
    """Test that checkpoint restores don't share references with original state."""
    class State(TypedDict):
        data: Annotated[dict, operator.or_]

    def modify(state: State) -> dict:
        state["data"]["nested"]["key"] = "modified"
        return {}

    graph = StateGraph(State)
    graph.add_node("modify", modify)
    graph.add_edge(START, "modify")
    graph.add_edge("modify", END)

    checkpointer = InMemorySaver()
    compiled = graph.compile(checkpointer=checkpointer)

    original_data = {"nested": {"key": "original"}}
    config = {"configurable": {"thread_id": "deep-copy-test"}}
    compiled.invoke({"data": original_data}, config)

    assert original_data["nested"]["key"] == "original"
```

**Step 2: Run test to verify it fails**

```bash
cd D:\PythonProjects\LiteGraph && python -m pytest tests/test_advanced.py::test_checkpoint_deep_copy_nested -v
```
Expected: FAIL

**Step 3: Fix `_safe_copy`**

In `zerograph/channels/binop.py`, replace:

```python
def _safe_copy(value):
    import copy
    if value is MISSING:
        return value
    try:
        return copy.deepcopy(value)
    except Exception:
        return value
```

**Step 4: Run tests**

```bash
cd D:\PythonProjects\LiteGraph && python -m pytest tests/test_advanced.py::test_checkpoint_deep_copy_nested -v
```
Expected: PASS

**Step 5: Commit**

```bash
git add zerograph/channels/binop.py tests/test_advanced.py
git commit -m "fix: use deepcopy in _safe_copy to prevent shared references"
```

---

### Task 4: Fix SQLite JSON serialization type loss

**Files:**
- Modify: `zerograph/checkpoint/sqlite.py` (add custom encoder)
- Test: `tests/test_sqlite.py`

**Problem:** `json.dumps(checkpoint, default=str)` converts all non-serializable objects to strings. On restore, these are strings, not original objects.

**Step 1: Write test for complex object checkpointing**

Add to `tests/test_sqlite.py`:

```python
def test_sqlite_checkpoint_complex_types():
    """Test that SQLite checkpoint preserves complex types."""
    import tempfile

    class State(TypedDict):
        timestamp: str
        count: int

    def add_ts(state: State) -> dict:
        return {"timestamp": "2024-01-01T00:00:00", "count": state["count"] + 1}

    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        db_path = f.name

    try:
        graph = StateGraph(State)
        graph.add_node("add_ts", add_ts)
        graph.add_edge(START, "add_ts")
        graph.add_edge("add_ts", END)

        with SqliteSaver(db_path) as saver:
            compiled = graph.compile(checkpointer=saver)
            config = {"configurable": {"thread_id": "complex-test"}}
            result = compiled.invoke({"count": 0, "timestamp": ""}, config)
            assert result["count"] == 1
            assert result["timestamp"] == "2024-01-01T00:00:00"
    finally:
        import os
        os.unlink(db_path)
```

**Step 2: Run test**

```bash
cd D:\PythonProjects\LiteGraph && python -m pytest tests/test_sqlite.py::test_sqlite_checkpoint_complex_types -v
```

**Step 3: Fix JSON serialization**

In `zerograph/checkpoint/sqlite.py`, add after imports:

```python
from datetime import datetime, date

class _CheckpointEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, (datetime, date)):
            return {"__datetime__": obj.isoformat()}
        if isinstance(obj, set):
            return {"__set__": list(obj)}
        if isinstance(obj, bytes):
            return {"__bytes__": obj.hex()}
        return str(obj)

def _decode_checkpoint_obj(obj):
    if isinstance(obj, dict):
        if "__datetime__" in obj:
            return datetime.fromisoformat(obj["__datetime__"])
        if "__set__" in obj:
            return set(obj["__set__"])
        if "__bytes__" in obj:
            return bytes.fromhex(obj["__bytes__"])
    return obj

def _dump_json(obj) -> str:
    return json.dumps(obj, cls=_CheckpointEncoder)

def _load_json(text: str):
    return json.loads(text, object_hook=_decode_checkpoint_obj)
```

Replace all `json.dumps(..., default=str)` with `_dump_json(...)` and all `json.loads(...)` with `_load_json(...)`.

**Step 4: Run tests**

```bash
cd D:\PythonProjects\LiteGraph && python -m pytest tests/test_sqlite.py -v
```

**Step 5: Commit**

```bash
git add zerograph/checkpoint/sqlite.py tests/test_sqlite.py
git commit -m "fix: preserve complex types in SQLite checkpoint serialization"
```

---

## Phase 2: Medium Issues (correctness & code quality)

### Task 4: Fix `create_supervisor` variable reference order

**Files:**
- Modify: `zerograph/prebuilt/supervisor.py:92-116`

**Step 1: Move `agent_names_set` before `route_from_supervisor`**

```python
agent_names_set = set(agent_names)
path_map = {name: name for name in agent_names}
path_map[END] = END

def route_from_supervisor(state: dict) -> str | list[str]:
    ...
```

**Step 2: Run tests**

```bash
cd D:\PythonProjects\LiteGraph && python -m pytest tests/ -v -k supervisor
```

**Step 3: Commit**

```bash
git add zerograph/prebuilt/supervisor.py
git commit -m "fix: define agent_names_set before route_from_supervisor function"
```

---

### Task 5: Fix `LLMStreamAdapter` tool_calls index handling

**Files:**
- Modify: `zerograph/adapters/llm_stream.py:82-102`

**Step 1: Handle None index**

Change `idx = getattr(tc_delta, "index", len(self._tool_calls))` to:
```python
idx = getattr(tc_delta, "index", None)
if idx is None:
    idx = len(self._tool_calls)
```

**Step 2: Run tests**

```bash
cd D:\PythonProjects\LiteGraph && python -m pytest tests/test_llm_adapter.py -v
```

**Step 3: Commit**

```bash
git add zerograph/adapters/llm_stream.py
git commit -m "fix: handle sparse tool call indices in LLMStreamAdapter"
```

---

### Task 6: Fix `TAG_HIDDEN` LangSmith coupling

**Files:**
- Modify: `zerograph/constants.py:5`

**Step 1: Change to project-internal tag**

```python
TAG_HIDDEN = "zerograph:hidden"
```

**Step 2: Run all tests**

```bash
cd D:\PythonProjects\LiteGraph && python -m pytest tests/ -v
```

**Step 3: Commit**

```bash
git add zerograph/constants.py
git commit -m "chore: decouple TAG_HIDDEN from LangSmith naming"
```

---

## Phase 3: Minor Issues (polish)

### Task 7: Fix `__all__` consistency — export missing channel types

**Files:**
- Modify: `zerograph/__init__.py`

**Step 1: Add missing exports**

Add imports:
```python
from zerograph.channels.base import BaseChannel
from zerograph.channels.last_value import LastValue, LastValueAfterFinish
from zerograph.channels.ephemeral_value import EphemeralValue
from zerograph.channels.named_barrier import NamedBarrierValue, NamedBarrierValueAfterFinish
from zerograph.channels.topic import Topic
from zerograph.channels.binop import BinaryOperatorAggregate
```

Add to `__all__`:
```python
"BaseChannel", "LastValue", "LastValueAfterFinish",
"EphemeralValue", "NamedBarrierValue", "NamedBarrierValueAfterFinish",
"Topic", "BinaryOperatorAggregate",
```

**Step 2: Verify imports**

```bash
cd D:\PythonProjects\LiteGraph && python -c "from zerograph import BaseChannel, LastValueAfterFinish, NamedBarrierValue, Topic, BinaryOperatorAggregate, EphemeralValue; print('OK')"
```

**Step 3: Commit**

```bash
git add zerograph/__init__.py
git commit -m "chore: export all channel types from top-level package"
```

---

### Task 8: Add project URLs to pyproject.toml

**Files:**
- Modify: `pyproject.toml`

**Step 1: Add `[project.urls]` section**

```toml
[project.urls]
Homepage = "https://github.com/yourname/LiteGraph"
Repository = "https://github.com/yourname/LiteGraph"
Documentation = "https://yourname.github.io/LiteGraph"
Changelog = "https://github.com/yourname/LiteGraph/blob/main/CHANGELOG.md"
```

**Step 2: Commit**

```bash
git add pyproject.toml
git commit -m "chore: add project URLs to pyproject.toml"
```

---

### Task 8: Run full test suite and verify all fixes

**Step 1: Run complete test suite**

```bash
cd D:\PythonProjects\LiteGraph && python -m pytest tests/ -v --tb=short
```

**Step 2: Run test-project scenarios**

```bash
cd D:\PythonProjects\LiteGraph\test-project && python main.py
```

**Step 3: Final commit**

```bash
git commit --allow-empty -m "chore: all code review fixes verified by test suite"
```

---

## Summary of Changes

| Task | Severity | File(s) | Issue |
|------|----------|---------|-------|
| 1 | Critical | `binop.py` | `_safe_copy` uses shallow copy |
| 2 | Critical | `sqlite.py` | JSON `default=str` loses types |
| 3 | Medium | `supervisor.py` | Variable reference order (fragile) |
| 4 | Low | `llm_stream.py` | Sparse tool call indices |
| 5 | Minor | `constants.py` | `langsmith:` prefix in TAG_HIDDEN |
| 6 | Minor | `__init__.py` | Missing `__all__` exports |
| 7 | Minor | `pyproject.toml` | Missing project URLs |
| 8 | Verification | All | Full test suite run |

**Deferred to separate plan:**
- Split `_loop.py` (2278 lines) into smaller modules
- Eliminate sync/async code duplication (~40%)
- Unify PregelLoop with `_algo.py` Pregel algorithm
