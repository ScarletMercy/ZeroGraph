"""ToolNode — automatic tool execution node for LLM tool calling."""

from __future__ import annotations

import asyncio
import inspect
import json
from typing import Any, Callable, Union

__all__ = ("ToolNode", "InjectedState", "InjectedStore")


class _InjectedMarker:
    """Base class for injection markers. Used as type annotations."""

    def __class_getitem__(cls, item):
        return cls


class InjectedState(_InjectedMarker):
    """Marker annotation: inject the full graph state into this tool parameter.

    Usage::

        def my_tool(query: str, state: InjectedState) -> str:
            # ``state`` receives the full graph state dict
            return f"{query} in context of {state}"
    """


class InjectedStore(_InjectedMarker):
    """Marker annotation: inject the Store into this tool parameter.

    Usage::

        def my_tool(query: str, store: InjectedStore) -> str:
            # ``store`` receives the InMemoryStore instance
            return store.get("namespace", key)
    """


def _python_type_to_json(annotation: Any) -> str:
    """Map Python type annotation to JSON Schema type string."""
    if annotation is inspect.Parameter.empty:
        return "string"
    type_map = {
        str: "string",
        int: "integer",
        float: "number",
        bool: "boolean",
        list: "array",
        dict: "object",
    }
    if annotation in type_map:
        return type_map[annotation]
    origin = getattr(annotation, "__origin__", None)
    if origin is not None:
        if getattr(origin, "__name__", None) == "Union" or origin is Union:
            from typing import get_args
            args = [a for a in get_args(annotation) if a is not type(None)]
            if len(args) == 1:
                return _python_type_to_json(args[0])
        return type_map.get(origin, "string")
    return "string"


def _get_callable_name(func: Callable) -> str:
    return getattr(func, "name", None) or getattr(func, "__name__", None) or func.__class__.__name__


def _extract_schema(func: Callable) -> dict:
    """Extract JSON Schema from a function's signature and docstring."""
    sig = inspect.signature(func)
    properties: dict[str, Any] = {}
    required: list[str] = []
    for name, param in sig.parameters.items():
        # Skip injected parameters (InjectedState, InjectedStore)
        hint = param.annotation
        if isinstance(hint, type) and issubclass(hint, _InjectedMarker):
            continue
        if param.kind in (inspect.Parameter.VAR_POSITIONAL, inspect.Parameter.VAR_KEYWORD):
            continue
        if param.default is inspect.Parameter.empty:
            required.append(name)
        prop: dict[str, Any] = {"type": _python_type_to_json(param.annotation)}
        properties[name] = prop
    return {
        "name": _get_callable_name(func),
        "description": (func.__doc__ or "").strip().split("\n")[0],
        "parameters": {
            "type": "object",
            "properties": properties,
            "required": required,
        },
    }


class ToolNode:
    """Node that executes tools based on LLM tool_calls in the last message.

    Usage::

        tools = [search_tool, calculator_tool]
        graph.add_node("tools", ToolNode(tools))
    """

    def __init__(
        self,
        tools: list[Callable],
        *,
        handle_errors: bool = True,
        store: Any = None,
    ) -> None:
        self.tools_by_name: dict[str, Callable] = {}
        for t in tools:
            name = _get_callable_name(t)
            self.tools_by_name[name] = t
        self.handle_errors = handle_errors
        self.store = store

    def _inject_args(
        self, tool_fn: Callable, tool_args: dict, state: dict
    ) -> dict:
        """Detect InjectedState / InjectedStore annotations and add them."""
        hints = {}
        try:
            from typing import get_type_hints
            hints = get_type_hints(tool_fn)
        except Exception:
            pass
        sig = inspect.signature(tool_fn)
        for pname, param in sig.parameters.items():
            hint = hints.get(pname)
            if hint is None:
                hint = param.annotation
            if isinstance(hint, type) and issubclass(hint, _InjectedMarker):
                if issubclass(hint, InjectedState):
                    tool_args[pname] = state
                elif issubclass(hint, InjectedStore):
                    tool_args[pname] = self.store
        return tool_args

    def __call__(self, state: dict) -> dict:
        """Execute tools synchronously."""
        messages = state.get("messages", [])
        last_msg = messages[-1] if messages else {}
        tool_calls = last_msg.get("tool_calls", [])

        results: list[dict] = []
        for tc in tool_calls:
            tc_id = tc.get("id", "")
            try:
                tool_name = tc["function"]["name"]
                tool_args = json.loads(tc["function"]["arguments"])
            except (KeyError, TypeError, json.JSONDecodeError) as e:
                if self.handle_errors:
                    results.append(
                        _make_tool_message(tc_id, "", f"Invalid tool call: {e}", is_error=True)
                    )
                    continue
                raise
            try:
                tool_fn = self.tools_by_name[tool_name]
            except KeyError:
                if self.handle_errors:
                    results.append(
                        _make_tool_message(tc_id, tool_name, f"Unknown tool: {tool_name}", is_error=True)
                    )
                    continue
                raise
            try:
                tool_args = self._inject_args(tool_fn, tool_args, state)
                output = tool_fn(**tool_args)
            except Exception as e:
                if self.handle_errors:
                    results.append(
                        _make_tool_message(tc_id, tool_name, str(e), is_error=True)
                    )
                    continue
                raise
            results.append(_make_tool_message(tc_id, tool_name, output))

        return {"messages": results}

    async def ainvoke(self, state: dict) -> dict:
        """Execute tools asynchronously — supports async tool functions."""
        messages = state.get("messages", [])
        last_msg = messages[-1] if messages else {}
        tool_calls = last_msg.get("tool_calls", [])

        results: list[dict] = []
        for tc in tool_calls:
            tc_id = tc.get("id", "")
            try:
                tool_name = tc["function"]["name"]
                tool_args = json.loads(tc["function"]["arguments"])
            except (KeyError, TypeError, json.JSONDecodeError) as e:
                if self.handle_errors:
                    results.append(
                        _make_tool_message(tc_id, "", f"Invalid tool call: {e}", is_error=True)
                    )
                    continue
                raise
            try:
                tool_fn = self.tools_by_name[tool_name]
            except KeyError:
                if self.handle_errors:
                    results.append(
                        _make_tool_message(tc_id, tool_name, f"Unknown tool: {tool_name}", is_error=True)
                    )
                    continue
                raise
            try:
                tool_args = self._inject_args(tool_fn, tool_args, state)
                if asyncio.iscoroutinefunction(tool_fn):
                    output = await tool_fn(**tool_args)
                else:
                    output = tool_fn(**tool_args)
            except Exception as e:
                if self.handle_errors:
                    results.append(
                        _make_tool_message(tc_id, tool_name, str(e), is_error=True)
                    )
                    continue
                raise
            results.append(_make_tool_message(tc_id, tool_name, output))

        return {"messages": results}

    @staticmethod
    def inject_tools(tools: list[Callable]) -> list[dict]:
        """Generate OpenAI function calling format tools parameter.

        Usage::

            tools_param = ToolNode.inject_tools([search, calc])
            response = client.chat.completions.create(
                model="gpt-4", messages=msgs, tools=tools_param
            )
        """
        return [
            {"type": "function", "function": _extract_schema(t)}
            for t in tools
        ]


def _make_tool_message(
    tool_call_id: str,
    name: str,
    content: Any,
    *,
    is_error: bool = False,
) -> dict:
    return {
        "role": "tool",
        "name": name,
        "content": str(content),
        "tool_call_id": tool_call_id,
        "is_error": is_error,
    }
