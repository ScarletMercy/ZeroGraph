# 中断与恢复

本教程介绍如何在图执行过程中暂停，等待外部输入后恢复执行。

## 核心概念

中断机制允许你在图的执行过程中暂停，等待用户输入或其他外部信号后再继续。这对于以下场景非常有用：

- **人工审核**：在执行敏感操作前等待人工确认
- **收集信息**：执行过程中向用户提问
- **分步执行**：长时间运行的任务分步完成

## interrupt_after：节点后中断

最简单的中断方式是在指定节点执行后暂停：

```python
from zerograph import StateGraph, START, END, InMemorySaver

graph = StateGraph(dict)
graph.add_node("process", lambda s: {"status": "已处理"})
graph.add_node("review", lambda s: {"reviewed": True})
graph.add_edge(START, "process")
graph.add_edge("process", "review")
graph.add_edge("review", END)

checkpointer = InMemorySaver()
app = graph.compile(
    checkpointer=checkpointer,
    interrupt_after=["process"]  # process 节点执行后暂停
)

config = {"configurable": {"thread_id": "1"}}

# 第一次调用 — 在 process 后暂停
result = app.invoke({"status": ""}, config)
print(result)  # {'status': '已处理'}，但 review 还没执行

# 恢复执行
result = app.invoke(None, config)
print(result)  # {'status': '已处理', 'reviewed': True}
```

## interrupt_before：节点前中断

在指定节点执行前暂停：

```python
app = graph.compile(
    checkpointer=checkpointer,
    interrupt_before=["review"]  # 进入 review 节点前暂停
)
```

## interrupt()：节点内部中断

在节点函数内部调用 `interrupt()` 实现更精细的控制：

```python
from zerograph import StateGraph, START, END, InMemorySaver, interrupt

def human_review(state: dict) -> dict:
    # 暂停执行，返回值会作为提示信息
    answer = interrupt("请确认是否继续？")
    return {"approved": answer}

graph = StateGraph(dict)
graph.add_node("review", human_review)
graph.add_edge(START, "review")
graph.add_edge("review", END)

checkpointer = InMemorySaver()
app = graph.compile(checkpointer=checkpointer)
config = {"configurable": {"thread_id": "1"}}

# 第一次调用 — 在 interrupt() 处暂停
result = app.invoke({"approved": False}, config)
print(result)  # {'approved': False}

# 用 Command(resume=...) 传入恢复值
from zerograph import Command
result = app.invoke(Command(resume=True), config)
print(result)  # {'approved': True}
```

### interrupt() 的工作流程

1. 第一次调用 `app.invoke(input, config)` → 在 `interrupt()` 处抛出 `GraphInterrupt`
2. 调用 `app.invoke(Command(resume=value), config)` → `value` 成为 `interrupt()` 的返回值
3. 图从断点继续执行

## 多轮中断

一个图可以有多个 `interrupt()` 调用，每次恢复只推进到下一个中断点：

```python
def multi_step(state: dict) -> dict:
    step1 = interrupt("第一步：请输入名称")
    step2 = interrupt("第二步：请确认")
    return {"name": step1, "confirmed": step2}
```

恢复时按顺序提供每个值：

```python
app.invoke(Command(resume="Alice"), config)   # 推进到第二个 interrupt
app.invoke(Command(resume=True), config)      # 完成
```

## interrupt_after 与 interrupt() 的对比

| 特性 | `interrupt_after` | `interrupt()` |
|------|-------------------|---------------|
| 控制粒度 | 节点级别 | 函数内部任意位置 |
| 需要修改节点代码 | 否 | 是 |
| 传递恢复值 | `None` 或 `Command(resume=...)` | `Command(resume=value)` |
| 适用场景 | 简单暂停/恢复 | 需要收集用户输入 |

## 查看中断状态

```python
snapshot = app.get_state(config)
print(snapshot.interrupts)  # 中断信息列表
print(snapshot.next)        # 下一步要执行的节点
```

## 完整示例：人工审核工作流

```python
from zerograph import StateGraph, START, END, InMemorySaver, interrupt, Command

class State(dict):
    pass

def generate(state: dict) -> dict:
    return {"draft": "这是一份待审核的文档..."}

def review(state: dict) -> dict:
    approved = interrupt(f"请审核以下内容:\n{state['draft']}\n\n是否批准？")
    return {"approved": approved}

def publish(state: dict) -> dict:
    if state["approved"]:
        return {"status": "已发布"}
    return {"status": "已拒绝"}

graph = StateGraph(State)
graph.add_node("generate", generate)
graph.add_node("review", review)
graph.add_node("publish", publish)
graph.add_edge(START, "generate")
graph.add_edge("generate", "review")
graph.add_edge("review", "publish")
graph.add_edge("publish", END)

checkpointer = InMemorySaver()
app = graph.compile(checkpointer=checkpointer)
config = {"configurable": {"thread_id": "doc-1"}}

# 步骤 1: 执行到 review 中断
result = app.invoke({}, config)
print(result["draft"])  # 这是一份待审核的文档...

# 步骤 2: 查看中断信息
snapshot = app.get_state(config)
print(snapshot.next)  # ('review',)

# 步骤 3: 传入审核结果并恢复
result = app.invoke(Command(resume=True), config)
print(result["status"])  # 已发布
```

## 下一步

- [子图嵌套](subgraphs.md) — 在子图中使用中断
- [错误处理与重试](error-handling.md) — 处理执行中的异常
