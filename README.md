# ZeroGraph

A lightweight graph execution engine for building stateful, multi-step workflows — with **zero external dependencies**.

Inspired by [LangGraph](https://github.com/langchain-ai/langgraph), ZeroGraph implements a Pregel-like superstep execution model with checkpointing, streaming, subgraph support, and prebuilt LLM agent patterns, all in pure Python.

## Features

- **StateGraph** — Define workflows as typed state machines with nodes, edges, and conditional routing
- **Channel System** — Flexible state management via reducers (`LastValue`, `BinaryOperatorAggregate`, `AnyValue`, `Topic`, etc.)
- **Checkpointing** — Persist and resume execution with `InMemorySaver` and `SqliteSaver` (sync + async)
- **Streaming** — Multiple stream modes: `values`, `updates`, `custom`, `messages`, `checkpoints`, `tasks`
- **Subgraphs** — Nest graphs within graphs with namespace-isolated state
- **Interrupt & Resume** — Pause execution at any node and resume later with user input
- **Functional API** — `@entrypoint` and `@task` decorators for workflow-style definitions
- **Cache & Store** — Node-level result caching (with TTL) and long-term key-value memory
- **Prebuilt Agents** — `ToolNode`, `create_react_agent`, `create_supervisor`, `create_swarm`
- **LLM Streaming** — `LLMStreamAdapter` for OpenAI/Anthropic-style chunk streaming
- **Visualization** — Generate Mermaid diagrams from any `StateGraph`

## Installation

```bash
pip install zerograph
```

## Quick Start

### A Simple Linear Graph

```python
from typing import Annotated, TypedDict
import operator
from zerograph import StateGraph, START, END

class State(TypedDict):
    messages: Annotated[list, operator.add]

def greet(state: State) -> dict:
    return {"messages": ["Hello!"]}

def bye(state: State) -> dict:
    return {"messages": ["Goodbye!"]}

graph = StateGraph(State)
graph.add_node("greet", greet)
graph.add_node("bye", bye)
graph.add_edge(START, "greet")
graph.add_edge("greet", "bye")
graph.add_edge("bye", END)

app = graph.compile()
result = app.invoke({"messages": []})
print(result["messages"])  # ['Hello!', 'Goodbye!']
```

### Conditional Routing

```python
from zerograph import StateGraph, START, END

def router(state: dict) -> str:
    if state["x"] > 0:
        return "positive"
    return "negative"

graph = StateGraph(dict)
graph.add_node("positive", lambda s: {"label": "pos"})
graph.add_node("negative", lambda s: {"label": "neg"})
graph.add_conditional_edges(START, router, {"positive": "positive", "negative": "negative"})
graph.add_edge("positive", END)
graph.add_edge("negative", END)

app = graph.compile()
print(app.invoke({"x": 5}))   # {'x': 5, 'label': 'pos'}
print(app.invoke({"x": -3}))  # {'x': -3, 'label': 'neg'}
```

### Streaming

```python
app = graph.compile()
for event in app.stream({"messages": []}, stream_mode="updates"):
    print(event)
# {'greet': {'messages': ['Hello!']}}
# {'bye': {'messages': ['Goodbye!']}}
```

### Checkpointing & Interrupt

```python
from zerograph import StateGraph, START, END, InMemorySaver, interrupt

def human_review(state: dict) -> dict:
    answer = interrupt("Please review and confirm:")
    return {"approved": answer}

graph = StateGraph(dict)
graph.add_node("review", human_review)
graph.add_edge(START, "review")
graph.add_edge("review", END)

checkpointer = InMemorySaver()
app = graph.compile(checkpointer=checkpointer, interrupt_after=["review"])

# First call — pauses at "review"
config = {"configurable": {"thread_id": "1"}}
result = app.invoke({"approved": False}, config)

# Resume with user input
result = app.invoke(Command(resume=True), config)
print(result["approved"])  # True
```

### ReAct Agent

```python
from zerograph import create_react_agent

def search(query: str) -> str:
    """Search the web."""
    return f"Result for {query}"

def calculator(expr: str) -> float:
    """Evaluate a math expression."""
    return eval(expr)

def llm_call(messages, tools=None):
    # Your LLM integration here
    ...

agent = create_react_agent(llm_call, [search, calculator])
result = agent.invoke({"messages": [{"role": "user", "content": "What is 2+2?"}]})
```

## API Overview

### Core

| Symbol | Description |
|--------|-------------|
| `StateGraph` | Graph builder with typed state |
| `CompiledStateGraph` | Compiled, executable graph |
| `START`, `END` | Special node constants |
| `Send` | Dynamic fan-out to specific nodes |
| `Command` | Update state and control flow |

### Execution

| Symbol | Description |
|--------|-------------|
| `interrupt()` | Pause execution from within a node |
| `entrypoint()` | Decorator for functional API entry points |
| `task()` | Decorator for discrete work units |

### Checkpointing

| Symbol | Description |
|--------|-------------|
| `BaseCheckpointSaver` | Abstract checkpoint backend |
| `InMemorySaver` | In-memory checkpoint storage |
| `SqliteSaver` | SQLite-backed checkpoint storage |
| `AsyncSqliteSaver` | Async SQLite checkpoint storage |

### State & Types

| Symbol | Description |
|--------|-------------|
| `add_messages` | Reducer for message lists (upsert by ID) |
| `RemoveMessage` | Marker to remove a message by ID |
| `RetryPolicy` | Configurable retry with exponential backoff |
| `TimeoutPolicy` | Per-node timeout configuration |

### Prebuilt Agents

| Symbol | Description |
|--------|-------------|
| `ToolNode` | Node that executes tool calls |
| `create_react_agent` | Build a ReAct loop in one call |
| `create_supervisor` | Build a supervisor multi-agent graph |
| `create_swarm` | Build a swarm with handoff-based routing |
| `LLMStreamAdapter` | Stream adapter for OpenAI/Anthropic chunks |

### Infrastructure

| Symbol | Description |
|--------|-------------|
| `BaseCache` / `InMemoryCache` | Node-level result caching with TTL |
| `BaseStore` / `InMemoryStore` | Long-term key-value memory |

## Requirements

- Python >= 3.11
- No external dependencies

## License

[MIT](LICENSE)
