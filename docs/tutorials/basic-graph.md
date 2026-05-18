# 基础图构建

本教程介绍 ZeroGraph 的核心概念：节点、边、状态和图的编译执行。

## 核心概念

ZeroGraph 的执行模型基于 **Pregel 超步**：

1. 图由**节点**和**边**组成
2. 所有节点共享一个**状态**（类似全局黑板）
3. 每个超步中，所有就绪的节点并行执行
4. 节点读取状态、返回更新，更新合并到状态后进入下一步
5. 直到没有更多节点需要执行，图结束

## 创建第一个图

### 定义状态

状态可以用任何 `TypedDict` 或普通 `dict`：

```python
from zerograph import StateGraph, START, END

graph = StateGraph(dict)
```

也可以用 `TypedDict` 做类型约束：

```python
from typing import TypedDict

class MyState(TypedDict):
    name: str
    count: int

graph = StateGraph(MyState)
```

### 添加节点

节点是普通函数，接收当前状态字典，返回要更新的字段：

```python
def step1(state: dict) -> dict:
    return {"name": "Alice"}

def step2(state: dict) -> dict:
    return {"count": state.get("count", 0) + 1}

graph.add_node("step1", step1)
graph.add_node("step2", step2)
```

!!! note "节点命名规则"
    - 节点名称不能重复
    - 可以用函数名自动命名：`graph.add_node(step1)` 等价于 `graph.add_node("step1", step1)`

### 添加边

边定义节点之间的执行顺序：

```python
graph.add_edge(START, "step1")   # 入口 → step1
graph.add_edge("step1", "step2") # step1 → step2
graph.add_edge("step2", END)     # step2 → 终止
```

### 编译与执行

```python
app = graph.compile()
result = app.invoke({"name": "", "count": 0})
print(result)  # {'name': 'Alice', 'count': 1}
```

## 方法链式调用

所有 `add_*` 和 `set_*` 方法都返回 `self`，支持链式调用：

```python
app = (
    StateGraph(dict)
    .add_node("a", lambda s: {"x": 1})
    .add_node("b", lambda s: {"x": 2})
    .add_edge(START, "a")
    .add_edge("a", "b")
    .add_edge("b", END)
    .compile()
)
```

## add_sequence 快捷方式

当多个节点需要顺序执行时，可以用 `add_sequence`：

```python
graph = StateGraph(dict)
graph.add_sequence([
    ("step_1", lambda s: {"step": 1}),
    ("step_2", lambda s: {"step": 2}),
    ("step_3", lambda s: {"step": 3}),
])
graph.add_edge(START, "step_1")
```

也可以指定节点名称：

```python
graph.add_sequence([
    ("first", lambda s: {"v": 1}),
    ("second", lambda s: {"v": 2}),
])
```

## 多起始节点

一个图可以有多个从 `START` 出发的边，实现并行起始：

```python
graph.add_node("a", lambda s: {"x": 1})
graph.add_node("b", lambda s: {"y": 2})
graph.add_edge(START, "a")
graph.add_edge(START, "b")
graph.add_edge("a", END)
graph.add_edge("b", END)
```

此时 `a` 和 `b` 在第一个超步中并行执行。

## 等待边（Fan-in）

等待边允许一个节点等待多个前置节点全部完成后才执行：

```python
graph.add_edge(["a", "b"], "merge")  # a 和 b 都完成后才执行 merge
```

## 简单根状态

状态不一定非要是 TypedDict。如果只需要传递单个值（如 `int`、`str`），可以直接用该类型：

```python
graph = StateGraph(int)

def inc(state: int) -> int:
    return state + 1

graph.add_node("inc", inc)
graph.add_edge(START, "inc")
graph.add_edge("inc", END)

app = graph.compile()
print(app.invoke(5))  # 6
```

!!! warning "根状态的节点返回值"
    当状态是根类型（非 TypedDict）时，节点直接返回新值（而非字典）。

## 完整示例

```python
from zerograph import StateGraph, START, END

def main():
    graph = StateGraph(dict)
    graph.add_node("input", lambda s: {"text": "Hello, " + s.get("name", "")})
    graph.add_node("upper", lambda s: {"text": s["text"].upper()})
    graph.add_node("output", lambda s: {"result": f"最终结果: {s['text']}"})

    graph.add_edge(START, "input")
    graph.add_edge("input", "upper")
    graph.add_edge("upper", "output")
    graph.add_edge("output", END)

    app = graph.compile()
    result = app.invoke({"name": "World"})
    print(result["result"])  # 最终结果: HELLO, WORLD

main()
```

## 下一步

- [状态与 Reducer](state-and-reducers.md) — 学习如何用 Reducer 管理复杂状态
- [条件分支与动态路由](conditional-routing.md) — 根据条件动态选择执行路径
