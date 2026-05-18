# 条件分支与动态路由

本教程介绍如何使用条件边、`Send` 和 `Command` 实现动态路由。

## 条件边：add_conditional_edges

条件边根据路由函数的返回值决定下一步执行哪个节点。

### 基本用法

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

### path_map 参数

`path_map` 定义路由函数返回值到节点名的映射：

```python
graph.add_conditional_edges(
    "decider",
    router,
    {"a": "node_a", "b": "node_b", END: END}
)
```

也可以传列表（返回值即节点名）：

```python
graph.add_conditional_edges("decider", router, ["node_a", "node_b"])
```

如果不传 `path_map`，路由函数返回值直接作为节点名。

### 条件入口点

```python
graph.set_conditional_entry_point(
    router,
    {"positive": "pos_node", "negative": "neg_node"}
)
```

## Send：动态扇出

`Send` 允许在运行时动态地将数据发送到指定节点，实现 map-reduce 模式。

### 基本用法

```python
from zerograph import StateGraph, START, END, Send

def route_to_workers(state: dict) -> list[Send]:
    topics = state["topics"]
    return [Send("worker", {"topic": t}) for t in topics]

def worker(state: dict) -> dict:
    return {"results": [f"处理了: {state['topic']}"]}

def merge_results(state: dict) -> dict:
    return {"summary": f"共处理 {len(state.get('results', []))} 个"}

graph = StateGraph(dict)
graph.add_node("worker", worker)
graph.add_node("merge", merge_results)
graph.add_conditional_edges(START, route_to_workers)
graph.add_edge("worker", "merge")
graph.add_edge("merge", END)

app = graph.compile()
result = app.invoke({"topics": ["AI", "Web", "Data"]})
```

### Send 的工作机制

1. 路由函数返回 `Send` 对象列表
2. 每个 `Send("node", arg)` 会触发一次目标节点的执行，`arg` 作为节点输入
3. 所有 Send 的结果汇总后合并到状态

## Command：更新状态 + 控制流

`Command` 同时支持更新状态和指定下一步要执行的节点。

### goto：指定下一步

```python
from zerograph import StateGraph, START, END, Command

def decider(state: dict) -> Command:
    if state.get("retry"):
        return Command(goto="process", update={"retry": False})
    return Command(goto=END)

def process(state: dict) -> dict:
    return {"result": "done"}

graph = StateGraph(dict)
graph.add_node("decider", decider)
graph.add_node("process", process)
graph.add_edge(START, "decider")
graph.add_edge("process", "decider")
graph.add_edge("decider", END)  # 需要有 END 的路径

app = graph.compile()
```

### Command.goto 的三种值

| 值 | 含义 |
|----|------|
| `"node_name"` | 跳转到指定节点 |
| `Send(...)` | 发送到指定节点 |
| `[Send(...), "node"]` | 同时发送多个 |

### destinations 参数

可以在 `add_node` 时预声明节点的合法目标，配合 Command 使用：

```python
graph.add_node("check", check_fn, destinations={"retry": "process", "done": END})
```

## Overwrite：绕过 Reducer

当状态字段有 Reducer 时，有时需要直接覆盖而非合并。使用 `Overwrite`：

```python
from zerograph import StateGraph, START, END, Overwrite
from typing import Annotated
import operator

class State(dict):
    items: Annotated[list, operator.add]

def reset(state: dict) -> dict:
    return {"items": Overwrite([])}  # 直接替换而非追加

graph = StateGraph(State)
graph.add_node("add", lambda s: {"items": ["a", "b"]})
graph.add_node("reset", reset)
graph.add_edge(START, "add")
graph.add_edge("add", "reset")
graph.add_edge("reset", END)

app = graph.compile()
result = app.invoke({"items": []})
print(result["items"])  # [] — 被覆盖为空列表
```

## 下一步

- [流式输出](streaming.md) — 实时观察图的执行过程
- [中断与恢复](interrupt-resume.md) — 在执行中暂停等待用户输入
