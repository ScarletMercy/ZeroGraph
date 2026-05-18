"""create_react_agent — one-function ReAct agent builder."""

from __future__ import annotations

from typing import Annotated, Any, Callable, TypedDict, get_type_hints

from zerograph.channels.messages import add_messages
from zerograph.constants import END, START
from zerograph.graph.state import StateGraph

__all__ = ("create_react_agent",)


def create_react_agent(
    llm_callable: Callable,
    tools: list[Callable],
    *,
    state_schema: type | None = None,
    checkpointer: Any | None = None,
    max_iterations: int = 25,
) -> Any:
    """Create a ReAct agent graph with minimal code.

    Usage::

        agent = create_react_agent(llm_call, [search_tool, calc_tool])
        result = agent.invoke({"messages": [{"role": "user", "content": "..."}]})

    Args:
        llm_callable: Function(messages, tools=...) -> message_dict.
            Must accept a list of message dicts and an optional ``tools``
            keyword argument with OpenAI-format tool schemas.
            Must return a dict with at least ``role`` and ``content``.
        tools: List of tool functions. Each function's name, docstring,
            and signature are used to generate the tools schema.
        state_schema: Optional TypedDict for state. Defaults to
            ``{messages: Annotated[list, add_messages]}``.
        checkpointer: Optional checkpoint saver instance.

    Returns:
        CompiledStateGraph ready for ``.invoke()`` / ``.stream()``.
    """
    from zerograph.prebuilt.tool_node import ToolNode

    if state_schema is None:

        class AgentState(TypedDict):
            messages: Annotated[list, add_messages]

        state_schema = AgentState

    tool_node = ToolNode(tools)
    tools_schema = ToolNode.inject_tools(tools)

    graph = StateGraph(state_schema)

    def agent_node(state: dict) -> dict:
        response = llm_callable(state["messages"], tools=tools_schema)
        return {"messages": [response]}

    def should_continue(state: dict) -> str:
        messages = state.get("messages", [])
        # Count agent turns (every 2 messages = 1 agent turn: agent+tool or agent+user)
        agent_turns = sum(1 for m in messages if m.get("role") == "assistant")
        if agent_turns >= max_iterations:
            return END
        last = messages[-1] if messages else {}
        if last.get("tool_calls"):
            return "tools"
        return END

    graph.add_node("agent", agent_node)
    graph.add_node("tools", tool_node)
    graph.add_edge(START, "agent")
    graph.add_conditional_edges(
        "agent",
        should_continue,
        {"tools": "tools", END: END},
    )
    graph.add_edge("tools", "agent")

    return graph.compile(checkpointer=checkpointer)
