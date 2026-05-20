"""LLM streaming adapter — convert SDK chunks to ZeroGraph messages stream."""

from __future__ import annotations

from collections.abc import Callable, Generator
from typing import Any

__all__ = ("LLMStreamAdapter", "stream_openai")


class LLMStreamAdapter:
    """Convert LLM SDK streaming chunks to ZeroGraph-compatible output.

    Works with OpenAI and Anthropic SDK chunk formats via duck typing
    (no SDK dependency required).

    Usage (non-streaming node)::

        def my_node(state):
            adapter = LLMStreamAdapter()
            for chunk in client.chat.completions.create(..., stream=True):
                adapter.append(chunk, provider="openai")
            return {"messages": [adapter.build_message()]}

    Usage (streaming node in "messages" mode)::

        def my_streaming_node(state):
            adapter = LLMStreamAdapter()
            for chunk in client.chat.completions.create(..., stream=True):
                delta = adapter.append(chunk, provider="openai")
                if delta:
                    yield delta
            return {"messages": [adapter.build_message()]}
    """

    def __init__(self) -> None:
        self._content_parts: list[str] = []
        self._role: str = "assistant"
        self._tool_calls: list[dict] = []

    def append(self, chunk: Any, *, provider: str = "openai") -> str | None:
        """Process one streaming chunk and return text delta if any.

        Args:
            chunk: A streaming chunk from the LLM SDK.
            provider: Either ``"openai"`` or ``"anthropic"``.

        Returns:
            The text content delta from this chunk, or None.
        """
        if provider == "openai":
            return self._process_openai_chunk(chunk)
        elif provider == "anthropic":
            return self._process_anthropic_chunk(chunk)
        raise ValueError(f"Unsupported provider: {provider}")

    def build_message(self) -> dict:
        """Build the final message dict from accumulated chunks."""
        msg: dict[str, Any] = {
            "role": self._role,
            "content": "".join(self._content_parts),
        }
        if self._tool_calls:
            msg["tool_calls"] = self._tool_calls
        return msg

    def _process_openai_chunk(self, chunk: Any) -> str | None:
        choices = getattr(chunk, "choices", None)
        if not choices:
            return None
        delta = getattr(choices[0], "delta", None)
        if delta is None:
            return None

        content = getattr(delta, "content", None)
        if content:
            self._content_parts.append(content)
            return content

        tc_list = getattr(delta, "tool_calls", None)
        if tc_list:
            for tc_delta in tc_list:
                idx = getattr(tc_delta, "index", None)
                if idx is None:
                    idx = len(self._tool_calls)
                fn = getattr(tc_delta, "function", None)
                while len(self._tool_calls) <= idx:
                    self._tool_calls.append(
                        {
                            "id": "",
                            "type": "function",
                            "function": {"name": "", "arguments": ""},
                        }
                    )
                if fn:
                    fn_name = getattr(fn, "name", None)
                    fn_args = getattr(fn, "arguments", None)
                    if fn_name:
                        self._tool_calls[idx]["function"]["name"] = fn_name
                    if fn_args:
                        self._tool_calls[idx]["function"]["arguments"] += fn_args
                tc_id = getattr(tc_delta, "id", None)
                if tc_id:
                    self._tool_calls[idx]["id"] = tc_id

        return None

    def _process_anthropic_chunk(self, chunk: Any) -> str | None:
        event_type = getattr(chunk, "type", None)
        if event_type == "content_block_delta":
            delta = getattr(chunk, "delta", None)
            if delta:
                # Text content delta
                text = getattr(delta, "text", None)
                if text:
                    self._content_parts.append(text)
                    return text
                # Tool input JSON delta
                if getattr(delta, "type", None) == "input_json_delta":
                    idx = getattr(delta, "index", 0)
                    partial = getattr(delta, "partial_json", "")
                    while len(self._tool_calls) <= idx:
                        self._tool_calls.append(
                            {"id": "", "type": "function", "function": {"name": "", "arguments": ""}}
                        )
                    self._tool_calls[idx]["function"]["arguments"] += partial
        elif event_type == "message_start":
            msg = getattr(chunk, "message", None)
            if msg:
                self._role = getattr(msg, "role", "assistant")
        elif event_type == "content_block_start":
            block = getattr(chunk, "content_block", None)
            if block and getattr(block, "type", None) == "tool_use":
                idx = getattr(block, "index", len(self._tool_calls))
                while len(self._tool_calls) <= idx:
                    self._tool_calls.append(
                        {
                            "id": "",
                            "type": "function",
                            "function": {"name": "", "arguments": ""},
                        }
                    )
                self._tool_calls[idx]["id"] = getattr(block, "id", "")
                self._tool_calls[idx]["function"]["name"] = getattr(
                    block, "name", ""
                )
        return None


def stream_openai(
    llm_callable: Callable,
    messages: list[dict],
    *,
    tools: list[dict] | None = None,
    **kwargs: Any,
) -> Generator[str, None, dict]:
    """Convenience generator: call OpenAI SDK streaming, yield text deltas.

    Use inside a generator node for ``"messages"`` stream mode::

        def my_node(state):
            result = yield from stream_openai(
                lambda **kw: client.chat.completions.create(**kw),
                state["messages"],
            )
            return result

    The generator yields text deltas.  Its *return value* (captured via
    ``result = yield from ...``) is the state update dict — you must
    ``return result`` so the framework receives it.
    """
    adapter = LLMStreamAdapter()
    params: dict[str, Any] = {"messages": messages, "stream": True, **kwargs}
    if tools:
        params["tools"] = tools
    stream = llm_callable(**params)
    for chunk in stream:
        delta = adapter.append(chunk, provider="openai")
        if delta:
            yield delta
    return {"messages": [adapter.build_message()]}
