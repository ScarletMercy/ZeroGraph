# 快速上手

本页带你在 5 分钟内掌握 ZeroGraph 的核心用法。

## 安装

=== "pip"

    ```bash
    pip install zerograph
    ```

=== "uv"

    ```bash
    uv add ZeroGraph
    ```

## 第一个图

ZeroGraph 的核心是 `StateGraph`。你定义状态模式、添加节点和边，然后编译执行。

```python
from typing import Annotated
import operator
from zerograph import StateGraph, START, END

# 1. 定义状态模式
class State(dict):
    messages: Annotated[list, operator.add]

# 2. 定义节点函数
def greet(state: dict) -> dict:
    return {"messages": ["Hello!"]}

def bye(state: dict) -> dict:
    return {"messages": ["Goodbye!"]}

# 3. 构建图
graph = StateGraph(State)
graph.add_node("greet", greet)
graph.add_node("bye", bye)
graph.add_edge(START, "greet")
graph.add_edge("greet", "bye")
graph.add_edge("bye", END)

# 4. 编译并执行
app = graph.compile()
result = app.invoke({"messages": []})
print(result["messages"])  # ['Hello!', 'Goodbye!']
```

## 理解关键概念

### 节点

节点是一个普通函数，接收当前状态，返回状态更新（字典）：

```python
def my_node(state: dict) -> dict:
    return {"key": "new_value"}  # 合并到状态中
```

### 边

- **直接边**：`graph.add_edge("A", "B")` — A 执行完后自动执行 B
- **条件边**：`graph.add_conditional_edges("A", router, {"x": "B", "y": "C"})` — 根据路由函数动态选择
- **特殊节点**：`START` 是入口，`END` 是终止

### Reducer

当多个节点需要更新同一个 key 时，用 `Annotated` 指定合并策略：

```python
from typing import Annotated
import operator

class State(dict):
    items: Annotated[list, operator.add]  # 累加而非覆盖
```

## 执行与流式输出

### 同步执行

```python
result = app.invoke({"messages": []})
```

### 流式输出

```python
for event in app.stream({"messages": []}, stream_mode="updates"):
    print(event)
# {'greet': {'messages': ['Hello!']}}
# {'bye': {'messages': ['Goodbye!']}}
```

### 异步执行

```python
result = await app.ainvoke({"messages": []})
async for event in app.astream({"messages": []}):
    print(event)
```

## 下一步

- [基础图构建](tutorials/basic-graph.md) — 深入了解节点、边和状态
- [条件分支与动态路由](tutorials/conditional-routing.md) — 动态路由和 Send 扇出
- [检查点与持久化](tutorials/checkpointing.md) — 保存和恢复执行状态
- [构建 ReAct 智能体](how-to/react-agent.md) — 用一行代码构建 Agent
