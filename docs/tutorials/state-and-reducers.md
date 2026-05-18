# 状态与 Reducer

本教程介绍 ZeroGraph 的状态管理机制，包括默认行为和自定义 Reducer。

## 状态的工作原理

ZeroGraph 中所有节点共享一个**状态字典**。每个节点执行后返回的字典会合并到状态中。

默认行为是 **LastValue**（最后值覆盖）——后执行的节点覆盖先执行的值。

## 默认行为：LastValue

```python
from zerograph import StateGraph, START, END

graph = StateGraph(dict)
graph.add_node("a", lambda s: {"x": 1})
graph.add_node("b", lambda s: {"x": 2})
graph.add_edge(START, "a")
graph.add_edge(START, "b")
graph.add_edge("a", END)
graph.add_edge("b", END)

app = graph.compile()
result = app.invoke({})
print(result["x"])  # 2（b 覆盖了 a 的值）
```

!!! warning "并行节点的冲突"
    当两个并行节点写入同一个 key 且使用 LastValue 时，后执行的会覆盖。如需累加，请使用 Reducer。

## 自定义 Reducer：Annotated 类型

使用 `typing.Annotated` 为字段指定 Reducer 函数：

```python
from typing import Annotated, TypedDict
import operator
from zerograph import StateGraph, START, END

class State(TypedDict):
    messages: Annotated[list, operator.add]  # 用 + 合并而非覆盖
    count: int                                # 默认 LastValue

def node_a(state: State) -> dict:
    return {"messages": ["来自A"], "count": 1}

def node_b(state: State) -> dict:
    return {"messages": ["来自B"], "count": 2}

graph = StateGraph(State)
graph.add_node("a", node_a)
graph.add_node("b", node_b)
graph.add_edge(START, "a")
graph.add_edge(START, "b")
graph.add_edge("a", END)
graph.add_edge("b", END)

app = graph.compile()
result = app.invoke({"messages": [], "count": 0})
print(result["messages"])  # ['来自A', '来自B'] — 两个都保留了
print(result["count"])     # 2 — LastValue，被覆盖
```

## 自定义 Reducer 函数

Reducer 是一个接收两个参数的函数 `(old, new) -> merged`：

```python
def max_reducer(existing: int, new_val: int) -> int:
    return max(existing, new_val)

class State(TypedDict):
    score: Annotated[int, max_reducer]

graph = StateGraph(State)
graph.add_node("a", lambda s: {"score": 10})
graph.add_node("b", lambda s: {"score": 30})
graph.add_node("c", lambda s: {"score": 20})
graph.add_edge(START, "a")
graph.add_edge("a", "b")
graph.add_edge("b", "c")
graph.add_edge("c", END)

app = graph.compile()
result = app.invoke({"score": 0})
print(result["score"])  # 30（取最大值）
```

## add_messages Reducer

ZeroGraph 内置了 `add_messages`——专为消息列表设计的 Reducer，支持按 ID 去重更新和删除：

```python
from typing import Annotated, TypedDict
from zerograph import StateGraph, START, END, add_messages, RemoveMessage

class ChatState(TypedDict):
    messages: Annotated[list, add_messages]

def add_new(state: ChatState) -> dict:
    return {"messages": [{"id": "1", "role": "user", "content": "Hi"}]}

def update_existing(state: ChatState) -> dict:
    # 相同 ID 的消息会被更新
    return {"messages": [{"id": "1", "role": "user", "content": "Hello!"}]}

def remove_one(state: ChatState) -> dict:
    return {"messages": [RemoveMessage(id="1")]}
```

### add_messages 的三条规则

| 操作 | 方式 |
|------|------|
| **新增** | 消息没有 `id` 或 `id` 不在已有列表中 → 追加 |
| **更新** | 消息的 `id` 与已有消息相同 → 替换 |
| **删除** | 使用 `RemoveMessage(id=...)` → 移除该消息 |

## BinaryOperatorAggregate

`Annotated[list, operator.add]` 本质上是 `BinaryOperatorAggregate` 的语法糖。你也可以直接使用：

```python
from zerograph.channels.binop import BinaryOperatorAggregate

# 等价于 Annotated[list, operator.add]
# BinaryOperatorAggregate(list, operator.add)
```

所有带两个位置参数的可调用对象都可以作为 Reducer：

| Reducer | 行为 |
|---------|------|
| `operator.add` | 列表拼接、数字相加 |
| `lambda a, b: a + b` | 自定义合并 |
| `set.union` | 集合合并 |
| `max_reducer` | 自定义逻辑 |

## 下一步

- [条件分支与动态路由](conditional-routing.md) — 根据状态动态选择路径
- [流式输出](streaming.md) — 实时获取图执行的中间结果
