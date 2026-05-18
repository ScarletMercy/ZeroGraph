"""create_supervisor — build a supervisor multi-agent graph."""

from __future__ import annotations

from typing import Annotated, Any, Callable, TypedDict

from zerograph.channels.messages import add_messages
from zerograph.constants import END, START
from zerograph.graph.state import StateGraph

__all__ = ("create_supervisor",)


def _default_agent_name(agent, index: int) -> str:
    if hasattr(agent, "__name__"):
        return agent.__name__
    if hasattr(agent, "builder") and hasattr(agent.builder, "state_schema"):
        return f"agent_{index}"
    return f"agent_{index}"


def create_supervisor(
    llm_callable: Callable,
    agents: list[Any],
    *,
    state_schema: type | None = None,
    checkpointer: Any | None = None,
) -> Any:
    """Create a supervisor multi-agent graph.

    The supervisor node calls ``llm_callable(messages, tools=...)`` to decide
    which agent to route to next.  Each agent runs and returns to the
    supervisor.  The loop continues until the LLM response contains no
    ``tool_calls`` with agent names (indicating it wants to finish).

    Args:
        llm_callable: ``fn(messages, tools=...) -> message_dict``.
            Must return a dict with ``role``, ``content``, and optionally
            ``tool_calls``.  When the supervisor wants to invoke an agent it
            returns a ``tool_call`` whose ``function.name`` matches the agent
            name.
        agents: List of agent callables or CompiledStateGraph instances.
        state_schema: Optional state TypedDict. Defaults to
            ``{messages: Annotated[list, add_messages]}``.
        checkpointer: Optional checkpoint saver.

    Returns:
        CompiledStateGraph ready for ``.invoke()`` / ``.stream()``.
    """
    if state_schema is None:

        class AgentState(TypedDict):
            messages: Annotated[list, add_messages]

        state_schema = AgentState

    agent_names: list[str] = []
    for i, agent in enumerate(agents):
        name = _default_agent_name(agent, i)
        # Ensure uniqueness
        base = name
        counter = 1
        while name in agent_names:
            name = f"{base}_{counter}"
            counter += 1
        agent_names.append(name)

    # Build tool schemas for the supervisor LLM
    agent_tools = [
        {
            "type": "function",
            "function": {
                "name": name,
                "description": f"Delegate work to agent '{name}'.",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "input": {
                            "type": "string",
                            "description": "The task or message to pass to the agent.",
                        }
                    },
                    "required": ["input"],
                },
            },
        }
        for name in agent_names
    ]

    graph = StateGraph(state_schema)

    def supervisor_node(state: dict) -> dict:
        response = llm_callable(state["messages"], tools=agent_tools)
        return {"messages": [response]}

    def route_from_supervisor(state: dict) -> str:
        messages = state.get("messages", [])
        last = messages[-1] if messages else {}
        tool_calls = last.get("tool_calls", [])
        if tool_calls:
            tc = tool_calls[0]
            name = tc["function"]["name"]
            if name in agent_names_set:
                return name
        return END

    agent_names_set = set(agent_names)
    path_map = {name: name for name in agent_names}
    path_map[END] = END

    graph.add_node("supervisor", supervisor_node)
    graph.add_edge(START, "supervisor")
    graph.add_conditional_edges("supervisor", route_from_supervisor, path_map)

    for name, agent in zip(agent_names, agents):
        if hasattr(agent, "invoke"):
            # CompiledStateGraph or similar
            def _make_agent_wrapper(ag):
                def wrapper(state):
                    return ag.invoke(state)
                return wrapper
            graph.add_node(name, _make_agent_wrapper(agent))
        else:
            graph.add_node(name, agent)
        graph.add_edge(name, "supervisor")

    return graph.compile(checkpointer=checkpointer)
