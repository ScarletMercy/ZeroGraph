"""create_swarm — build a swarm multi-agent graph with handoff-based routing."""

from __future__ import annotations

import asyncio
from typing import Annotated, Any, Callable, TypedDict

from zerograph.channels.messages import add_messages
from zerograph.constants import END, START
from zerograph.graph.state import StateGraph
from zerograph.prebuilt.tool_node import ToolNode

__all__ = ("create_swarm",)


def _tag_agent_name(name: str, agent: Callable) -> Callable:
    """Wrap an agent to tag its output messages with the agent's node name."""
    if asyncio.iscoroutinefunction(agent) or asyncio.iscoroutinefunction(getattr(agent, "__call__", None)):
        async def wrapper(state: dict) -> dict:
            result = await agent(state)
            return _tag_messages(name, result)
    else:
        def wrapper(state: dict) -> dict:
            result = agent(state)
            return _tag_messages(name, result)

    wrapper.__name__ = name
    return wrapper


def _tag_messages(name: str, result: dict) -> dict:
    """Add _agent_name to assistant messages in the result."""
    if isinstance(result, dict) and "messages" in result:
        tagged_msgs = []
        for msg in result["messages"]:
            if isinstance(msg, dict) and msg.get("role") == "assistant":
                msg = {**msg, "_agent_name": name}
            tagged_msgs.append(msg)
        return {**result, "messages": tagged_msgs}
    return result


def create_swarm(
    agents: list[Callable],
    tools: list[Callable] | None = None,
    *,
    state_schema: type | None = None,
    checkpointer: Any | None = None,
) -> Any:
    """Create a swarm multi-agent graph.

    Each agent is a callable ``fn(state) -> dict``.  An agent signals a
    *handoff* by returning a message dict with a ``"handoff"`` key whose value
    is the name of the next agent.  If no handoff is signalled the swarm
    terminates.

    Optional shared ``tools`` are available to every agent via a shared
    ``"tools"`` node (ToolNode).

    Args:
        agents: List of agent callables.
        tools: Optional list of tool functions shared by all agents.
        state_schema: Optional state TypedDict.
        checkpointer: Optional checkpoint saver.

    Returns:
        CompiledStateGraph.
    """
    if not agents:
        raise ValueError("agents list must not be empty")

    if state_schema is None:

        class AgentState(TypedDict):
            messages: Annotated[list, add_messages]

        state_schema = AgentState

    # Build agent names — use __name__ with index fallback for uniqueness
    agent_names: list[str] = []
    reserved = {"tools"} if tools else set()
    for i, agent in enumerate(agents):
        name = getattr(agent, "__name__", None)
        if not name or name in agent_names or name in reserved:
            name = f"agent_{i}"
        base = name
        counter = 1
        while name in agent_names or name in reserved:
            name = f"{base}_{counter}"
            counter += 1
        agent_names.append(name)

    agent_name_set = set(agent_names)
    first_agent = agent_names[0]

    graph = StateGraph(state_schema)

    for name, agent in zip(agent_names, agents):
        if hasattr(agent, "invoke"):
            def _make_agent_wrapper(ag, n):
                def wrapper(state):
                    return _tag_messages(n, ag.invoke(state))
                wrapper.__name__ = n
                return wrapper
            graph.add_node(name, _make_agent_wrapper(agent, name))
        else:
            tagged = _tag_agent_name(name, agent)
            graph.add_node(name, tagged)

    # Shared tools node (optional)
    if tools:
        tool_node = ToolNode(tools)
        graph.add_node("tools", tool_node)

    for name in agent_names:
        def _make_router(n):
            def router(state: dict) -> str:
                messages = state.get("messages", [])
                last = messages[-1] if messages else {}

                # If last message is a tool result, route back to the calling agent
                if last.get("role") == "tool":
                    for msg in reversed(messages):
                        if msg.get("tool_calls") and msg.get("role") == "assistant":
                            caller = msg.get("_agent_name")
                            if caller and caller in agent_name_set:
                                return caller
                            return n
                    return n

                # Check for tool_calls from assistant
                if last.get("tool_calls") and tools:
                    return "tools"

                # Check for handoff — only accept from the current agent's
                # own assistant message to prevent stale handoffs from
                # causing infinite loops when an agent returns empty messages.
                if last.get("role") == "assistant" and last.get("_agent_name") == n:
                    handoff = last.get("handoff")
                    if handoff and handoff in agent_name_set:
                        return handoff

                return END

            return router

        # All agents are valid targets (including self for tool results)
        targets = {n: n for n in agent_names}
        targets[END] = END
        if tools:
            targets["tools"] = "tools"

        graph.add_conditional_edges(name, _make_router(name), targets)

    if tools:
        # After tools execute, route back to the agent that called them
        def _tools_router(state: dict) -> str:
            messages = state.get("messages", [])
            for msg in reversed(messages):
                if msg.get("tool_calls") and msg.get("role") == "assistant":
                    caller = msg.get("_agent_name")
                    if caller and caller in agent_name_set:
                        return caller
                    break
            return first_agent

        tools_targets = {n: n for n in agent_names}
        graph.add_conditional_edges("tools", _tools_router, tools_targets)

    graph.add_edge(START, first_agent)

    return graph.compile(checkpointer=checkpointer)
