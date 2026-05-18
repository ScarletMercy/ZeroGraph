# 检查点与持久化

本教程介绍如何使用检查点系统保存和恢复图的执行状态。

## 为什么需要检查点

- **持久化**：图执行中断后可以从上次的位置恢复
- **状态回放**：查看每一步的状态变化历史
- **人工干预**：配合中断功能实现人机交互
- **调试**：追溯每步的状态快照

## InMemorySaver

最基本的检查点存储，数据保存在内存中：

```python
from zerograph import StateGraph, START, END, InMemorySaver

graph = StateGraph(dict)
graph.add_node("step1", lambda s: {"count": s.get("count", 0) + 1})
graph.add_edge(START, "step1")
graph.add_edge("step1", END)

checkpointer = InMemorySaver()
app = graph.compile(checkpointer=checkpointer)

# 每次调用使用同一个 thread_id
config = {"configurable": {"thread_id": "thread-1"}}
result = app.invoke({"count": 0}, config)
print(result)  # {'count': 1}

result = app.invoke(result, config)
print(result)  # {'count': 2}
```

!!! note "thread_id"
    `thread_id` 是检查点的隔离维度。不同的 thread_id 拥有独立的状态历史。

## get_state：查看当前状态

```python
snapshot = app.get_state(config)
print(snapshot.values)      # 当前状态值
print(snapshot.next)        # 下一步要执行的节点
print(snapshot.created_at)  # 检查点创建时间
print(snapshot.metadata)    # 元数据（source, step）
```

### 查看子图状态

```python
snapshot = app.get_state(config, subgraphs=True)
print(snapshot.subgraphs)  # 子图的状态快照
```

## get_state_history：查看历史

```python
history = app.get_state_history(config, limit=10)
for snapshot in history:
    print(f"Step {snapshot.metadata['step']}: {snapshot.values}")
```

历史记录按时间倒序排列（最新的在前）。

## update_state：手动修改状态

```python
# 直接修改状态值
app.update_state(config, {"count": 100})

# 指定"以某个节点的身份"修改（影响 next 字段）
app.update_state(config, {"count": 50}, as_node="step1")
```

`as_node` 参数会影响图恢复时下一步执行哪个节点。

## SqliteSaver

SQLite 支持持久化到磁盘，适合生产环境：

```python
from zerograph import StateGraph, START, END, SqliteSaver

graph = StateGraph(dict)
graph.add_node("step", lambda s: {"v": s.get("v", 0) + 1})
graph.add_edge(START, "step")
graph.add_edge("step", END)

# 使用上下文管理器
with SqliteSaver("my_checkpoints.db") as saver:
    app = graph.compile(checkpointer=saver)
    config = {"configurable": {"thread_id": "t1"}}
    result = app.invoke({"v": 0}, config)
    print(result)  # {'v': 1}
```

### SqliteSaver 的特性

| 特性 | 说明 |
|------|------|
| **WAL 模式** | 默认开启，支持并发读写 |
| **线程安全** | 每个线程独立的数据库连接 |
| **上下文管理器** | `with SqliteSaver(...) as saver:` 自动管理连接 |
| **busy_timeout** | 默认 5 秒，避免锁等待 |

### 删除线程数据

```python
saver = SqliteSaver("checkpoints.db")
saver.delete_thread("thread-1")  # 删除该线程的所有检查点
saver.close()
```

## AsyncSqliteSaver

异步版本的 SQLite 检查点存储：

```python
from zerograph import AsyncSqliteSaver

async def run():
    async with AsyncSqliteSaver("checkpoints.db") as saver:
        app = graph.compile(checkpointer=saver)
        config = {"configurable": {"thread_id": "t1"}}
        result = await app.ainvoke({"v": 0}, config)
        print(result)

    # 也可以使用异步方法直接操作
    async with AsyncSqliteSaver("checkpoints.db") as saver:
        # aput / aget_tuple / alist / adelete_thread 等异步方法
        # 与同步版本用法相同，仅需 await
        cp_tuple = await saver.aget_tuple(config)
        history = await saver.alist(config, limit=5)
        await saver.adelete_thread("t1")
```

## 检查点与流式结合

```python
checkpointer = InMemorySaver()
app = graph.compile(checkpointer=checkpointer)
config = {"configurable": {"thread_id": "1"}}

# 边执行边获取检查点事件
for event in app.stream({"x": 0}, config, stream_mode="checkpoints"):
    print(f"Checkpoint saved: {event}")
```

## 完整示例：有状态的计数器

```python
from zerograph import StateGraph, START, END, InMemorySaver

graph = StateGraph(dict)
graph.add_node("increment", lambda s: {"count": s.get("count", 0) + 1})
graph.add_edge(START, "increment")
graph.add_edge("increment", END)

checkpointer = InMemorySaver()
app = graph.compile(checkpointer=checkpointer)
config = {"configurable": {"thread_id": "counter"}}

# 模拟多次调用
for i in range(5):
    result = app.invoke({"count": 0}, config)
    print(f"第 {i+1} 次调用: count={result['count']}")

# 查看历史
history = app.get_state_history(config)
print(f"共有 {len(history)} 条检查点记录")
```

## 下一步

- [中断与恢复](interrupt-resume.md) — 检查点 + 中断实现人机交互
- [SQLite 检查点配置](../how-to/sqlite-checkpoint.md) — 生产环境的详细配置
