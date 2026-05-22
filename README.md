# ZeroGraph

一个轻量级图执行引擎，用于构建有状态的多步骤工作流 —— **零外部依赖**。

受 [LangGraph](https://github.com/langchain-ai/langgraph) 启发，ZeroGraph 实现了类似 Pregel 的超步执行模型，支持检查点、流式输出、子图支持和预构建 LLM 智能体模式，全部纯 Python 实现。

## 特性

- **StateGraph** —— 将工作流定义为带类型的状态机，支持节点、边和条件路由
- **通道系统** —— 通过归约器灵活管理状态（`LastValue`、`BinaryOperatorAggregate`、`AnyValue`、`Topic` 等）
- **检查点** —— 使用 `InMemorySaver` 和 `SqliteSaver`（同步 + 异步）持久化和恢复执行
- **流式输出** —— 多种流模式：`values`、`updates`、`custom`、`messages`、`checkpoints`、`tasks`
- **子图** —— 在图中嵌套图，状态按命名空间隔离
- **中断与恢复** —— 在任意节点暂停执行，稍后使用用户输入恢复
- **函数式 API** —— `@entrypoint` 和 `@task` 装饰器，工作流风格定义
- **缓存与存储** —— 节点级结果缓存（支持 TTL）和长期键值记忆
- **预构建智能体** —— `ToolNode`、`create_react_agent`、`create_supervisor`、`create_swarm`
- **LLM 流式** —— `LLMStreamAdapter` 适配 OpenAI/Anthropic 风格的分块流式
- **可视化** —— 从任意 `StateGraph` 生成 Mermaid 图

## 安装

```bash
pip install zerograph
```

## 快速开始

### 简单线性图

```python
from typing import Annotated, TypedDict
import operator
from zerograph import StateGraph, START, END

class State(TypedDict):
    messages: Annotated[list, operator.add]

def greet(state: State) -> dict:
    return {"messages": ["你好！"]}

def bye(state: State) -> dict:
    return {"messages": ["再见！"]}

graph = StateGraph(State)
graph.add_node("greet", greet)
graph.add_node("bye", bye)
graph.add_edge(START, "greet")
graph.add_edge("greet", "bye")
graph.add_edge("bye", END)

app = graph.compile()
result = app.invoke({"messages": []})
print(result["messages"])  # ['你好！', '再见！']
```

### 条件路由

```python
from zerograph import StateGraph, START, END

def router(state: dict) -> str:
    if state["x"] > 0:
        return "positive"
    return "negative"

graph = StateGraph(dict)
graph.add_node("positive", lambda s: {"label": "正"})
graph.add_node("negative", lambda s: {"label": "负"})
graph.add_conditional_edges(START, router, {"positive": "positive", "negative": "negative"})
graph.add_edge("positive", END)
graph.add_edge("negative", END)

app = graph.compile()
print(app.invoke({"x": 5}))   # {'x': 5, 'label': '正'}
print(app.invoke({"x": -3}))  # {'x': -3, 'label': '负'}
```

### 流式输出

```python
app = graph.compile()
for event in app.stream({"messages": []}, stream_mode="updates"):
    print(event)
# {'greet': {'messages': ['你好！']}}
# {'bye': {'messages': ['再见！']}}
```

### 检查点与中断

```python
from zerograph import StateGraph, START, END, InMemorySaver, interrupt
from zerograph.types import Command

def human_review(state: dict) -> dict:
    answer = interrupt("请审核并确认：")
    return {"approved": answer}

graph = StateGraph(dict)
graph.add_node("review", human_review)
graph.add_edge(START, "review")
graph.add_edge("review", END)

checkpointer = InMemorySaver()
app = graph.compile(checkpointer=checkpointer, interrupt_after=["review"])

# 第一次调用 —— 在 "review" 处暂停
config = {"configurable": {"thread_id": "1"}}
result = app.invoke({"approved": False}, config)

# 使用用户输入恢复
result = app.invoke(Command(resume=True), config)
print(result["approved"])  # True
```

### ReAct 智能体

```python
from zerograph import create_react_agent

def search(query: str) -> str:
    """搜索网页。"""
    return f"{query} 的结果"

def calculator(expr: str) -> float:
    """计算数学表达式。"""
    return eval(expr)

def llm_call(messages, tools=None):
    # 你的 LLM 集成代码
    ...

agent = create_react_agent(llm_call, [search, calculator])
result = agent.invoke({"messages": [{"role": "user", "content": "2+2 等于多少？"}]})
```

## API 概览

### 核心

| 符号 | 说明 |
|------|------|
| `StateGraph` | 带类型状态的图构建器 |
| `CompiledStateGraph` | 编译后的可执行图 |
| `START`, `END` | 特殊节点常量 |
| `Send` | 动态扇出到指定节点 |
| `Command` | 更新状态并控制流程 |

### 执行

| 符号 | 说明 |
|------|------|
| `interrupt()` | 从节点内部暂停执行 |
| `entrypoint()` | 函数式 API 入口点装饰器 |
| `task()` | 离散工作单元装饰器 |

### 检查点

| 符号 | 说明 |
|------|------|
| `BaseCheckpointSaver` | 抽象检查点后端 |
| `InMemorySaver` | 内存检查点存储 |
| `SqliteSaver` | SQLite 检查点存储 |
| `AsyncSqliteSaver` | 异步 SQLite 检查点存储 |

### 状态与类型

| 符号 | 说明 |
|------|------|
| `add_messages` | 消息列表归约器（按 ID 进行 upsert） |
| `RemoveMessage` | 按 ID 删除消息的标记 |
| `RetryPolicy` | 可配置的指数退避重试策略 |
| `TimeoutPolicy` | 每节点超时配置 |

### 预构建智能体

| 符号 | 说明 |
|------|------|
| `ToolNode` | 执行工具调用的节点 |
| `create_react_agent` | 一行构建 ReAct 循环 |
| `create_supervisor` | 构建主管多智能体图 |
| `create_swarm` | 构建基于 handoff 路由的群组 |
| `LLMStreamAdapter` | OpenAI/Anthropic 分块流适配器 |

### 基础设施

| 符号 | 说明 |
|------|------|
| `BaseCache` / `InMemoryCache` | 节点级结果缓存，支持 TTL |
| `BaseStore` / `InMemoryStore` | 长期键值记忆 |

## 要求

- Python >= 3.11
- 零外部依赖

## 许可证

[MIT](LICENSE)
