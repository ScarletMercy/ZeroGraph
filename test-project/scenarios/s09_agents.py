"""s09_agents — 多智能体测试：ToolNode / react_agent / supervisor / swarm。"""

from typing import Annotated, TypedDict
import operator

from zerograph import (
    START,
    END,
    StateGraph,
    Command,
    InMemorySaver,
)
from zerograph.prebuilt import (
    ToolNode,
    create_react_agent,
    create_supervisor,
    create_swarm,
)
from zerograph.prebuilt.tool_node import InjectedState


class St(TypedDict):
    messages: Annotated[list, operator.add]


# ---------------------------------------------------------------------------
# Helper: fake LLM that returns a tool_call on first turn, then final answer
# ---------------------------------------------------------------------------

def _make_fake_llm(tool_name: str, tool_args: dict, final_text: str):
    """Return a callable that acts like an LLM: first call → tool_call, second → text."""
    call_count = 0

    def fake_llm(messages, **kwargs):
        nonlocal call_count
        call_count += 1
        if call_count == 1:
            return {
                "role": "assistant",
                "content": "",
                "tool_calls": [{
                    "id": "tc_1",
                    "type": "function",
                    "function": {
                        "name": tool_name,
                        "arguments": str(tool_args).replace("'", '"'),
                    },
                }],
            }
        return {"role": "assistant", "content": final_text}

    return fake_llm


# ---------------------------------------------------------------------------
# Test 1: ToolNode basic — executes tools from tool_calls
# ---------------------------------------------------------------------------

def _test_tool_node() -> tuple[str, bool, str]:
    def add(a: int, b: int) -> int:
        """Add two numbers."""
        return a + b

    tool_node = ToolNode([add])
    state = {
        "messages": [{
            "role": "assistant",
            "tool_calls": [{
                "id": "tc_1",
                "type": "function",
                "function": {"name": "add", "arguments": '{"a": 3, "b": 5}'},
            }],
        }],
    }
    result = tool_node(state)
    msgs = result.get("messages", [])
    if len(msgs) == 1 and msgs[0]["content"] == "8":
        return ("ToolNode basic", True, "add(3,5) = 8")
    return ("ToolNode basic", False, f"unexpected: {result}")


# ---------------------------------------------------------------------------
# Test 2: ToolNode.inject_tools — schema generation
# ---------------------------------------------------------------------------

def _test_inject_tools() -> tuple[str, bool, str]:
    def search(query: str) -> str:
        """Search for something."""
        return query

    schemas = ToolNode.inject_tools([search])
    if len(schemas) == 1 and schemas[0]["type"] == "function":
        fn = schemas[0]["function"]
        if fn["name"] == "search" and "query" in fn["parameters"]["properties"]:
            return ("ToolNode.inject_tools", True,
                    f"schema generated: {fn['name']}")
    return ("ToolNode.inject_tools", False, f"unexpected: {schemas}")


# ---------------------------------------------------------------------------
# Test 3: InjectedState / InjectedStore markers
# ---------------------------------------------------------------------------

def _test_injected_markers() -> tuple[str, bool, str]:
    def my_tool(query: str, state: InjectedState) -> str:
        return f"{query} in {state}"

    import inspect
    sig = inspect.signature(my_tool)
    state_param = sig.parameters["state"]
    if isinstance(state_param.annotation, type) and issubclass(state_param.annotation, InjectedState):
        return ("InjectedState marker", True, "annotation detected correctly")
    return ("InjectedState marker", False,
            f"annotation: {state_param.annotation}")


# ---------------------------------------------------------------------------
# Test 4: create_react_agent — tool call then final answer
# ---------------------------------------------------------------------------

def _test_react_agent() -> tuple[str, bool, str]:
    def multiply(a: int, b: int) -> int:
        """Multiply two numbers."""
        return a * b

    fake_llm = _make_fake_llm("multiply", {"a": 3, "b": 7}, "Done: 21")

    try:
        app = create_react_agent(fake_llm, [multiply])
        result = app.invoke({"messages": [{"role": "user", "content": "calc"}]})
        messages = result.get("messages", [])
        # Should have: user msg, assistant tool_call, tool result, assistant final
        has_tool_result = any(
            m.get("role") == "tool" and m.get("content") == "21"
            for m in messages
        )
        has_final = any(
            m.get("role") == "assistant" and "Done" in (m.get("content") or "")
            for m in messages
        )
        if has_tool_result and has_final:
            return ("create_react_agent", True,
                    f"{len(messages)} messages, tool result=21")
        return ("create_react_agent", False,
                f"missing tool_result or final: {messages}")
    except Exception as exc:
        return ("create_react_agent", False, str(exc))


# ---------------------------------------------------------------------------
# Test 5: create_supervisor — delegates to agent
# ---------------------------------------------------------------------------

def _test_supervisor() -> tuple[str, bool, str]:
    def agent_fn(state):
        return {"messages": [{"role": "assistant", "content": "agent_reply"}]}

    call_count = 0

    def fake_llm(messages, **kwargs):
        nonlocal call_count
        call_count += 1
        if call_count == 1:
            # Route to agent_fn (name = "agent_fn" by default)
            return {
                "role": "assistant",
                "content": "",
                "tool_calls": [{
                    "id": "tc_1",
                    "type": "function",
                    "function": {
                        "name": "agent_fn",
                        "arguments": '{"input": "hello"}',
                    },
                }],
            }
        # After agent replies, supervisor returns final answer
        return {"role": "assistant", "content": "supervisor_done"}

    try:
        app = create_supervisor(fake_llm, [agent_fn])
        result = app.invoke({"messages": [{"role": "user", "content": "hello"}]})
        messages = result.get("messages", [])
        has_agent_reply = any(
            m.get("content") == "agent_reply" for m in messages
        )
        has_final = any(
            m.get("content") == "supervisor_done" for m in messages
        )
        if not has_agent_reply:
            return ("create_supervisor", False, f"no agent reply: {messages}")
        if not has_final:
            return ("create_supervisor", False,
                    f"agent replied but no supervisor final answer: {messages}")
        return ("create_supervisor", True,
                f"{len(messages)} messages, agent + supervisor both responded")
    except Exception as exc:
        return ("create_supervisor", False, str(exc))


# ---------------------------------------------------------------------------
# Test 6: create_swarm — handoff between agents
# ---------------------------------------------------------------------------

def _test_swarm() -> tuple[str, bool, str]:
    def agent_a(state):
        return {"messages": [{"role": "assistant", "content": "A_done", "handoff": "agent_b"}]}

    def agent_b(state):
        return {"messages": [{"role": "assistant", "content": "B_done"}]}

    try:
        app = create_swarm([agent_a, agent_b])
        result = app.invoke({"messages": [{"role": "user", "content": "go"}]})
        messages = result.get("messages", [])
        has_a = any(m.get("content") == "A_done" for m in messages)
        has_b = any(m.get("content") == "B_done" for m in messages)
        if not has_a:
            return ("create_swarm", False, f"agent_a never executed: {messages}")
        if not has_b:
            return ("create_swarm", False, f"handoff to agent_b failed: {messages}")
        return ("create_swarm", True,
                f"{len(messages)} messages, handoff A→B confirmed")
    except Exception as exc:
        return ("create_swarm", False, str(exc))


# ---------------------------------------------------------------------------
# Runner
# ---------------------------------------------------------------------------

def run() -> list[tuple[str, bool, str]]:
    results: list[tuple[str, bool, str]] = []
    for fn in (
        _test_tool_node,
        _test_inject_tools,
        _test_injected_markers,
        _test_react_agent,
        _test_supervisor,
        _test_swarm,
    ):
        try:
            results.append(fn())
        except Exception as exc:
            results.append((fn.__name__, False, str(exc)))
    return results
