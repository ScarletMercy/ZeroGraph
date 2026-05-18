# 子图嵌套

本教程介绍如何将一个编译后的图作为另一个图的节点，实现图的嵌套组合。

## 核心概念

子图（Subgraph）是将一个 `CompiledStateGraph` 作为节点添加到另一个 `StateGraph` 中。

- 外层图和内层图有**独立的命名空间**
- 状态通过输入/输出模式映射
- 检查点按命名空间层级隔离

## 基本用法

```python
from zerograph import StateGraph, START, END

# === 内层图 ===
inner = StateGraph(dict)
inner.add_node("transform", lambda s: {"value": s["value"] * 2})
inner.add_edge(START, "transform")
inner.add_edge("transform", END)
inner_app = inner.compile()

# === 外层图 ===
outer = StateGraph(dict)
outer.add_node("subgraph", inner_app)  # 直接传入 CompiledStateGraph
outer.add_node("post", lambda s: {"value": s["value"] + 1})
outer.add_edge(START, "subgraph")
outer.add_edge("subgraph", "post")
outer.add_edge("post", END)

app = outer.compile()
result = app.invoke({"value": 5})
print(result)  # {'value': 11}（5*2=10, 10+1=11）
```

### 执行流程

1. 外层图接收到 `{"value": 5}`
2. `subgraph` 节点将状态传给内层图
3. 内层图执行 `transform`：`5 * 2 = 10`
4. 内层图返回 `{"value": 10}`，合并到外层状态
5. 外层图执行 `post`：`10 + 1 = 11`

## 带条件路由的子图

子图内部可以有条件边，不影响外层图的路由逻辑：

```python
# 内层图：根据条件走不同路径
inner = StateGraph(dict)
inner.add_node("check", lambda s: {"checked": True})
inner.add_node("path_a", lambda s: {"result": "A"})
inner.add_node("path_b", lambda s: {"result": "B"})

def inner_route(state: dict) -> str:
    return "path_a" if state["value"] > 0 else "path_b"

inner.add_edge(START, "check")
inner.add_conditional_edges("check", inner_route)
inner.add_edge("path_a", END)
inner.add_edge("path_b", END)
inner_app = inner.compile()

# 外层图
outer = StateGraph(dict)
outer.add_node("sub", inner_app)
outer.add_edge(START, "sub")
outer.add_edge("sub", END)

app = outer.compile()
print(app.invoke({"value": 10}))   # {'value': 10, 'checked': True, 'result': 'A'}
print(app.invoke({"value": -5}))   # {'value': -5, 'checked': True, 'result': 'B'}
```

## 输入/输出模式

子图可以有独立的 `input_schema` 和 `output_schema`，控制传入和传出的字段：

```python
from typing import TypedDict

class FullState(TypedDict):
    name: str
    age: int
    score: int

class SubInput(TypedDict):
    score: int

class SubOutput(TypedDict):
    score: int

inner = StateGraph(SubInput, output_schema=SubOutput)
inner.add_node("boost", lambda s: {"score": s["score"] + 10})
inner.add_edge(START, "boost")
inner.add_edge("boost", END)
inner_app = inner.compile()
```

当子图作为节点被调用时，只有 `input_schema` 中定义的字段会传入子图。

## 检查点命名空间

子图的检查点在独立的命名空间中保存：

```
thread-1/checkpoint_ns/
  ├─ (外层检查点)
  ├─ subgraph|              # 子图命名空间
  │   ├─ (子图检查点)
  │   └─ nested|            # 嵌套更深层的子图
  │       └─ (最内层检查点)
```

### 查看子图状态

```python
snapshot = app.get_state(config, subgraphs=True)
print(snapshot.subgraphs)
# {'subgraph': StateSnapshot(values=..., next=..., ...)}
```

### 子图中的中断

中断在子图中也能正常工作，恢复时需要在正确的命名空间下操作。

## context_schema：不可变上下文注入

`context_schema` 允许你向所有节点注入只读的上下文值，这些值不会随图执行而改变：

```python
from typing import TypedDict
from zerograph import StateGraph, START, END

class MyContext(TypedDict):
    user_id: str
    api_key: str

class MyState(TypedDict):
    result: str

def my_node(state: dict, config: dict) -> dict:
    ctx = config.get("configurable", {}).get("__context__", {})
    user_id = ctx.get("user_id", "unknown")
    return {"result": f"处理用户 {user_id} 的请求"}

graph = StateGraph(MyState, context_schema=MyContext)
graph.add_node("process", my_node)
graph.add_edge(START, "process")
graph.add_edge("process", END)

app = graph.compile(context={"user_id": "alice", "api_key": "sk-xxx"})
result = app.invoke({"result": ""})
print(result["result"])  # 处理用户 alice 的请求
```

!!! note "context 与 state 的区别"
    - **state**：可变，节点可以读取和更新
    - **context**：不可变，在 `compile()` 时固定，所有节点只读访问

## Command.PARENT：子图向父图发送命令

子图中的节点可以使用 `Command(graph=Command.PARENT, update=...)` 将数据传递给父图：

```python
from zerograph import StateGraph, START, END, Command

# 子图：处理完成后将结果发回父图
inner = StateGraph(dict)
inner.add_node("process", lambda s: {"inner_result": s["value"] * 10})
inner.add_node("respond", lambda s: Command(
    graph=Command.PARENT,
    update={"parent_result": s["inner_result"]}
))
inner.add_edge(START, "process")
inner.add_edge("process", "respond")
inner_app = inner.compile()

# 父图
outer = StateGraph(dict)
outer.add_node("sub", inner_app)
outer.add_edge(START, "sub")
outer.add_edge("sub", END)

app = outer.compile()
result = app.invoke({"value": 5})
print(result)  # {'value': 5, 'inner_result': 50, 'parent_result': 50}
```

## 多层嵌套

ZeroGraph 支持任意深度的嵌套：

```python
# 最内层
level3 = StateGraph(dict)
level3.add_node("core", lambda s: {"v": s["v"] ** 2})
level3.add_edge(START, "core")
level3.add_edge("core", END)
l3_app = level3.compile()

# 中间层
level2 = StateGraph(dict)
level2.add_node("inner", l3_app)
level2.add_edge(START, "inner")
level2.add_edge("inner", END)
l2_app = level2.compile()

# 外层
level1 = StateGraph(dict)
level1.add_node("mid", l2_app)
level1.add_edge(START, "mid")
level1.add_edge("mid", END)
l1_app = level1.compile()

result = l1_app.invoke({"v": 3})  # 3 → 9
```

## 完整示例：数据处理管道

```python
from zerograph import StateGraph, START, END

# 预处理子图
preprocess = StateGraph(dict)
preprocess.add_node("clean", lambda s: {"text": s["text"].strip().lower()})
preprocess.add_node("split", lambda s: {"tokens": s["text"].split()})
preprocess.add_edge(START, "clean")
preprocess.add_edge("clean", "split")
preprocess.add_edge("split", END)
preprocess_app = preprocess.compile()

# 分析子图
analyze = StateGraph(dict)
analyze.add_node("count", lambda s: {"count": len(s.get("tokens", []))})
analyze.add_node("stats", lambda s: {"avg_len": sum(len(t) for t in s.get("tokens", [])) / max(s.get("count", 1), 1)})
analyze.add_edge(START, "count")
analyze.add_edge("count", "stats")
analyze.add_edge("stats", END)
analyze_app = analyze.compile()

# 主图
main = StateGraph(dict)
main.add_node("preprocess", preprocess_app)
main.add_node("analyze", analyze_app)
main.add_edge(START, "preprocess")
main.add_edge("preprocess", "analyze")
main.add_edge("analyze", END)

app = main.compile()
result = app.invoke({"text": "  Hello World Foo  "})
print(result["count"])    # 3
print(result["avg_len"])  # 4.0
```

## 下一步

- [函数式 API](functional-api.md) — 另一种定义工作流的方式
- [中断与恢复](interrupt-resume.md) — 在子图中使用中断
