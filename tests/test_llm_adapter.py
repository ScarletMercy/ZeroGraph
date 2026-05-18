"""Tests for LLM streaming adapter."""

import pytest

from zerograph.adapters.llm_stream import LLMStreamAdapter


class _MockDelta:
    def __init__(self, content=None, tool_calls=None):
        self.content = content
        self.tool_calls = tool_calls


class _MockChoice:
    def __init__(self, delta):
        self.delta = delta


class _MockChunk:
    def __init__(self, content=None, tool_calls=None):
        self.choices = [_MockChoice(_MockDelta(content=content, tool_calls=tool_calls))]


class _MockTCDelta:
    def __init__(self, index=0, id=None, name=None, arguments=None):
        self.index = index
        self.id = id

        class _Fn:
            pass
        self.function = _Fn()
        if name:
            self.function.name = name
        if arguments:
            self.function.arguments = arguments


class _MockAnthropicDelta:
    def __init__(self, text=None):
        self.text = text


class _MockAnthropicChunk:
    def __init__(self, event_type, **kwargs):
        self.type = event_type
        for k, v in kwargs.items():
            setattr(self, k, v)


class TestLLMStreamAdapter:

    def test_openai_text_chunks(self):
        adapter = LLMStreamAdapter()
        chunks = [
            _MockChunk(content="Hello"),
            _MockChunk(content=" world"),
            _MockChunk(content="!"),
        ]
        deltas = []
        for c in chunks:
            d = adapter.append(c, provider="openai")
            if d:
                deltas.append(d)

        assert deltas == ["Hello", " world", "!"]
        msg = adapter.build_message()
        assert msg["role"] == "assistant"
        assert msg["content"] == "Hello world!"

    def test_openai_empty_content(self):
        adapter = LLMStreamAdapter()
        chunk = _MockChunk(content=None)
        result = adapter.append(chunk, provider="openai")
        assert result is None
        assert adapter.build_message()["content"] == ""

    def test_openai_tool_calls(self):
        adapter = LLMStreamAdapter()
        chunks = [
            _MockChunk(
                tool_calls=[
                    _MockTCDelta(index=0, id="call_1", name="search", arguments=None)
                ]
            ),
            _MockChunk(
                tool_calls=[
                    _MockTCDelta(index=0, arguments='{"qu')
                ]
            ),
            _MockChunk(
                tool_calls=[
                    _MockTCDelta(index=0, arguments='ery": "test"}')
                ]
            ),
        ]
        for c in chunks:
            adapter.append(c, provider="openai")

        msg = adapter.build_message()
        assert len(msg["tool_calls"]) == 1
        assert msg["tool_calls"][0]["id"] == "call_1"
        assert msg["tool_calls"][0]["function"]["name"] == "search"
        assert msg["tool_calls"][0]["function"]["arguments"] == '{"query": "test"}'

    def test_openai_multiple_tool_calls(self):
        adapter = LLMStreamAdapter()
        chunks = [
            _MockChunk(
                tool_calls=[
                    _MockTCDelta(index=0, id="call_1", name="search"),
                    _MockTCDelta(index=1, id="call_2", name="calc"),
                ]
            ),
        ]
        for c in chunks:
            adapter.append(c, provider="openai")

        msg = adapter.build_message()
        assert len(msg["tool_calls"]) == 2
        assert msg["tool_calls"][0]["function"]["name"] == "search"
        assert msg["tool_calls"][1]["function"]["name"] == "calc"

    def test_anthropic_text_chunks(self):
        adapter = LLMStreamAdapter()
        # message_start
        adapter.append(
            _MockAnthropicChunk("message_start", message=type("M", (), {"role": "assistant"})()),
            provider="anthropic",
        )
        # content deltas
        deltas = []
        for text in ["Hello", " from", " Anthropic"]:
            d = adapter.append(
                _MockAnthropicChunk(
                    "content_block_delta",
                    delta=_MockAnthropicDelta(text=text),
                ),
                provider="anthropic",
            )
            if d:
                deltas.append(d)

        assert deltas == ["Hello", " from", " Anthropic"]
        msg = adapter.build_message()
        assert msg["content"] == "Hello from Anthropic"
        assert msg["role"] == "assistant"

    def test_anthropic_tool_use(self):
        adapter = LLMStreamAdapter()
        adapter.append(
            _MockAnthropicChunk(
                "content_block_start",
                content_block=type("B", (), {"type": "tool_use", "index": 0, "id": "tu_1", "name": "search"})(),
            ),
            provider="anthropic",
        )
        adapter.append(
            _MockAnthropicChunk(
                "content_block_delta",
                delta=type("D", (), {"type": "input_json_delta", "index": 0, "partial_json": '{"qu'})(),
            ),
            provider="anthropic",
        )
        adapter.append(
            _MockAnthropicChunk(
                "content_block_delta",
                delta=type("D", (), {"type": "input_json_delta", "index": 0, "partial_json": 'ery": "test"}'})(),
            ),
            provider="anthropic",
        )

        msg = adapter.build_message()
        assert len(msg["tool_calls"]) == 1
        assert msg["tool_calls"][0]["id"] == "tu_1"
        assert msg["tool_calls"][0]["function"]["name"] == "search"
        assert msg["tool_calls"][0]["function"]["arguments"] == '{"query": "test"}'

    def test_empty_stream(self):
        adapter = LLMStreamAdapter()
        msg = adapter.build_message()
        assert msg["content"] == ""
        assert msg["role"] == "assistant"
        assert "tool_calls" not in msg

    def test_unsupported_provider(self):
        adapter = LLMStreamAdapter()
        with pytest.raises(ValueError, match="Unsupported provider"):
            adapter.append(None, provider="gemini")

    def test_build_message_no_tool_calls(self):
        adapter = LLMStreamAdapter()
        adapter.append(_MockChunk(content="hi"), provider="openai")
        msg = adapter.build_message()
        assert "tool_calls" not in msg

    def test_build_message_with_tool_calls(self):
        adapter = LLMStreamAdapter()
        adapter.append(
            _MockChunk(tool_calls=[_MockTCDelta(index=0, id="c1", name="fn")]),
            provider="openai",
        )
        msg = adapter.build_message()
        assert "tool_calls" in msg
        assert len(msg["tool_calls"]) == 1


class TestStreamOpenai:

    def test_stream_openai_helper(self):
        from zerograph.adapters.llm_stream import stream_openai

        chunks = [_MockChunk(content="Hello"), _MockChunk(content=" world")]

        def mock_llm(**kwargs):
            return chunks

        gen = stream_openai(mock_llm, [{"role": "user", "content": "hi"}])
        deltas = list(gen)
        assert deltas == ["Hello", " world"]

        # The return value from the generator
        # In Python, generator return values are accessed via StopIteration.value
        # but list() doesn't capture that. Let's test differently.

    def test_stream_openai_with_return(self):
        from zerograph.adapters.llm_stream import stream_openai

        chunks = [_MockChunk(content="Hi")]

        def mock_llm(**kwargs):
            return chunks

        gen = stream_openai(mock_llm, [{"role": "user", "content": "hi"}])
        collected = []
        while True:
            try:
                collected.append(next(gen))
            except StopIteration as e:
                return_val = e.value
                break

        assert collected == ["Hi"]
        assert return_val is not None
        assert return_val["messages"][0]["content"] == "Hi"
