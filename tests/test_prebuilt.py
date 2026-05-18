"""Tests for ToolNode and create_react_agent."""

import asyncio
import pytest

from zerograph.prebuilt.tool_node import ToolNode, _extract_schema


def search_tool(query: str) -> str:
    """Search the web."""
    return f"Results for: {query}"


def calc_tool(a: int, b: int) -> int:
    """Add two numbers."""
    return a + b


def failing_tool(x: int) -> str:
    """A tool that always fails."""
    raise ValueError("Tool error!")


async def async_tool(query: str) -> str:
    """Async search tool."""
    return f"Async results for: {query}"


class TestToolNode:

    def test_basic(self):
        node = ToolNode([search_tool])
        state = {
            "messages": [
                {
                    "role": "assistant",
                    "tool_calls": [
                        {
                            "id": "tc1",
                            "function": {
                                "name": "search_tool",
                                "arguments": '{"query": "python"}',
                            },
                        }
                    ],
                }
            ]
        }
        result = node(state)
        assert len(result["messages"]) == 1
        msg = result["messages"][0]
        assert msg["role"] == "tool"
        assert msg["tool_call_id"] == "tc1"
        assert "Results for: python" in msg["content"]
        assert msg["is_error"] is False

    def test_multiple_tools(self):
        node = ToolNode([search_tool, calc_tool])
        state = {
            "messages": [
                {
                    "role": "assistant",
                    "tool_calls": [
                        {
                            "id": "tc1",
                            "function": {
                                "name": "search_tool",
                                "arguments": '{"query": "test"}',
                            },
                        },
                        {
                            "id": "tc2",
                            "function": {
                                "name": "calc_tool",
                                "arguments": '{"a": 3, "b": 4}',
                            },
                        },
                    ],
                }
            ]
        }
        result = node(state)
        assert len(result["messages"]) == 2
        assert "Results for" in result["messages"][0]["content"]
        assert "7" in result["messages"][1]["content"]

    def test_error_handling(self):
        node = ToolNode([failing_tool], handle_errors=True)
        state = {
            "messages": [
                {
                    "role": "assistant",
                    "tool_calls": [
                        {
                            "id": "tc1",
                            "function": {
                                "name": "failing_tool",
                                "arguments": '{"x": 1}',
                            },
                        }
                    ],
                }
            ]
        }
        result = node(state)
        assert len(result["messages"]) == 1
        assert result["messages"][0]["is_error"] is True
        assert "Tool error!" in result["messages"][0]["content"]

    def test_error_no_handle(self):
        node = ToolNode([failing_tool], handle_errors=False)
        state = {
            "messages": [
                {
                    "role": "assistant",
                    "tool_calls": [
                        {
                            "id": "tc1",
                            "function": {
                                "name": "failing_tool",
                                "arguments": '{"x": 1}',
                            },
                        }
                    ],
                }
            ]
        }
        with pytest.raises(ValueError, match="Tool error!"):
            node(state)

    def test_missing_tool(self):
        node = ToolNode([search_tool], handle_errors=True)
        state = {
            "messages": [
                {
                    "role": "assistant",
                    "tool_calls": [
                        {
                            "id": "tc1",
                            "function": {
                                "name": "nonexistent",
                                "arguments": "{}",
                            },
                        }
                    ],
                }
            ]
        }
        result = node(state)
        assert result["messages"][0]["is_error"] is True
        assert "Unknown tool" in result["messages"][0]["content"]

    def test_async_tool(self):
        node = ToolNode([async_tool])
        state = {
            "messages": [
                {
                    "role": "assistant",
                    "tool_calls": [
                        {
                            "id": "tc1",
                            "function": {
                                "name": "async_tool",
                                "arguments": '{"query": "test"}',
                            },
                        }
                    ],
                }
            ]
        }
        result = asyncio.run(node.ainvoke(state))
        assert len(result["messages"]) == 1
        assert "Async results" in result["messages"][0]["content"]

    def test_empty_tool_calls(self):
        node = ToolNode([search_tool])
        result = node({"messages": [{"role": "assistant", "content": "hi"}]})
        assert result["messages"] == []

    def test_no_messages(self):
        node = ToolNode([search_tool])
        result = node({})
        assert result["messages"] == []


class TestInjectTools:

    def test_schema_extraction(self):
        schemas = ToolNode.inject_tools([search_tool, calc_tool])
        assert len(schemas) == 2

        search_schema = schemas[0]
        assert search_schema["type"] == "function"
        fn = search_schema["function"]
        assert fn["name"] == "search_tool"
        assert "query" in fn["parameters"]["properties"]
        assert fn["parameters"]["required"] == ["query"]

        calc_schema = schemas[1]
        fn2 = calc_schema["function"]
        assert fn2["name"] == "calc_tool"
        assert set(fn2["parameters"]["required"]) == {"a", "b"}

    def test_schema_types(self):
        def typed_tool(name: str, count: int, ratio: float, active: bool) -> str:
            """A typed tool."""
            return ""

        schemas = ToolNode.inject_tools([typed_tool])
        props = schemas[0]["function"]["parameters"]["properties"]
        assert props["name"]["type"] == "string"
        assert props["count"]["type"] == "integer"
        assert props["ratio"]["type"] == "number"
        assert props["active"]["type"] == "boolean"


class TestReactAgent:

    def test_basic_react(self):
        from zerograph import StateGraph

        call_count = 0

        def mock_llm(messages, tools=None):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                return {
                    "role": "assistant",
                    "content": "",
                    "tool_calls": [
                        {
                            "id": "tc1",
                            "function": {
                                "name": "search_tool",
                                "arguments": '{"query": "python langgraph"}',
                            },
                        }
                    ],
                }
            return {
                "role": "assistant",
                "content": "Based on the search results, LangGraph is a framework.",
            }

        from zerograph.prebuilt.react_agent import create_react_agent
        agent = create_react_agent(mock_llm, [search_tool])
        result = agent.invoke(
            {"messages": [{"role": "user", "content": "What is LangGraph?"}]}
        )
        assert len(result["messages"]) == 4  # user + assistant(tc) + tool + assistant(final)
        assert "LangGraph" in result["messages"][-1]["content"]

    def test_no_tool_call(self):
        def mock_llm(messages, tools=None):
            return {
                "role": "assistant",
                "content": "Hello! How can I help?",
            }

        from zerograph.prebuilt.react_agent import create_react_agent
        agent = create_react_agent(mock_llm, [search_tool])
        result = agent.invoke(
            {"messages": [{"role": "user", "content": "Hi"}]}
        )
        assert len(result["messages"]) == 2  # user + assistant
        assert "Hello" in result["messages"][-1]["content"]

    def test_with_checkpoint(self):
        from zerograph import InMemorySaver
        from zerograph.prebuilt.react_agent import create_react_agent

        call_count = 0

        def mock_llm(messages, tools=None):
            nonlocal call_count
            call_count += 1
            return {"role": "assistant", "content": f"Response {call_count}"}

        checkpointer = InMemorySaver()
        agent = create_react_agent(mock_llm, [search_tool], checkpointer=checkpointer)

        result = agent.invoke(
            {"messages": [{"role": "user", "content": "Hi"}]},
            {"configurable": {"thread_id": "t1"}},
        )
        assert result["messages"][-1]["content"] == "Response 1"
