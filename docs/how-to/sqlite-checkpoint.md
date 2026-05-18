# SQLite 检查点配置

本指南详细说明如何配置和使用 SQLite 作为检查点持久化后端。

## SqliteSaver

### 基本用法

```python
from zerograph import StateGraph, START, END, SqliteSaver

graph = StateGraph(dict)
graph.add_node("step", lambda s: {"v": s.get("v", 0) + 1})
graph.add_edge(START, "step")
graph.add_edge("step", END)

# 方式 1: 上下文管理器（推荐）
with SqliteSaver("checkpoints.db") as saver:
    app = graph.compile(checkpointer=saver)
    config = {"configurable": {"thread_id": "t1"}}
    result = app.invoke({"v": 0}, config)
    print(result)  # {'v': 1}
```

### 方式 2: 手动管理

```python
saver = SqliteSaver("checkpoints.db")
try:
    app = graph.compile(checkpointer=saver)
    result = app.invoke({"v": 0}, config)
finally:
    saver.close()
```

### 内存数据库

使用 `:memory:` 创建纯内存的 SQLite 数据库：

```python
saver = SqliteSaver(":memory:")
```

!!! warning ":memory: 的限制"
    内存数据库不能在 `SqliteSaver` 实例之间共享数据。每个实例有独立的数据库。

## 线程安全

`SqliteSaver` 通过 `threading.local()` 为每个线程创建独立的数据库连接：

```python
import threading

saver = SqliteSaver("checkpoints.db")

def worker(thread_id):
    app = graph.compile(checkpointer=saver)
    config = {"configurable": {"thread_id": f"thread-{thread_id}"}}
    for i in range(10):
        result = app.invoke({"v": 0}, config)

threads = [threading.Thread(target=worker, args=(i,)) for i in range(5)]
for t in threads:
    t.start()
for t in threads:
    t.join()
```

## WAL 模式

`SqliteSaver` 默认启用 WAL（Write-Ahead Logging）模式：

- 允许并发读写（读不阻塞写，写不阻塞读）
- 提高性能，适合多线程环境
- 自动配置 `busy_timeout=5000`（5 秒锁等待）

## 异步版本：AsyncSqliteSaver

### 基本用法

```python
from zerograph import AsyncSqliteSaver

async def run():
    async with AsyncSqliteSaver("checkpoints.db") as saver:
        app = graph.compile(checkpointer=saver)
        config = {"configurable": {"thread_id": "t1"}}
        result = await app.ainvoke({"v": 0}, config)
```

### 异步方法

```python
async with AsyncSqliteSaver("checkpoints.db") as saver:
    # 异步写入检查点
    await saver.aput(config, checkpoint, metadata)

    # 异步读取
    cp_tuple = await saver.aget_tuple(config)

    # 异步列出历史
    history = await saver.alist(config, limit=10)

    # 异步删除线程
    await saver.adelete_thread("thread-1")

    # 异步读取 pending writes
    writes = await saver.aget_pending_writes(config)
```

### 实现原理

- 文件数据库：使用 `asyncio.to_thread()` 在线程池中执行同步 SQLite 操作
- 内存数据库：使用单线程 `ThreadPoolExecutor` 确保连接共享

## 数据持久化

文件数据库在关闭后数据仍然保留：

```python
# 写入数据
with SqliteSaver("persist.db") as saver:
    app = graph.compile(checkpointer=saver)
    app.invoke({"v": 0}, {"configurable": {"thread_id": "t1"}})

# 重新打开，数据仍在
with SqliteSaver("persist.db") as saver:
    app = graph.compile(checkpointer=saver)
    history = app.get_state_history({"configurable": {"thread_id": "t1"}})
    print(len(history))  # > 0
```

## 复杂类型序列化

`SqliteSaver` 使用 JSON 序列化，支持：

- 嵌套字典
- 列表
- 布尔值
- `None`
- `Interrupt` 对象（使用 `__interrupt__` 标记）
- 其他类型使用 `str()` 回退

## 线程管理

### 删除线程数据

```python
saver = SqliteSaver("checkpoints.db")
saver.delete_thread("thread-1")
saver.close()
```

### 列出检查点

```python
with SqliteSaver("checkpoints.db") as saver:
    checkpoints = saver.list(
        {"configurable": {"thread_id": "t1"}},
        limit=10,
        before={"configurable": {"thread_id": "t1", "checkpoint_id": "cp-123"}}
    )
    for cp in checkpoints:
        print(cp.checkpoint)
        print(cp.metadata)
```

## 完整示例：有状态的工作流

```python
from zerograph import StateGraph, START, END, SqliteSaver

class State(dict):
    pass

def step1(state: dict) -> dict:
    return {"step1_done": True, "count": state.get("count", 0) + 1}

def step2(state: dict) -> dict:
    return {"step2_done": True}

graph = StateGraph(State)
graph.add_node("step1", step1)
graph.add_node("step2", step2)
graph.add_edge(START, "step1")
graph.add_edge("step1", "step2")
graph.add_edge("step2", END)

with SqliteSaver("workflow.db") as saver:
    app = graph.compile(checkpointer=saver)
    config = {"configurable": {"thread_id": "workflow-1"}}

    # 第一次执行
    result = app.invoke({"count": 0}, config)
    print(result)  # {'count': 1, 'step1_done': True, 'step2_done': True}

    # 查看历史
    history = app.get_state_history(config)
    print(f"检查点数: {len(history)}")

    # 再次执行（状态延续）
    result = app.invoke({"count": 0}, config)
    # 注意：每次 invoke 会创建新的执行，不自动延续上次的 count

    # 清理
    saver.delete_thread("workflow-1")
```

## 参考文档

- [API: SqliteSaver](../api/checkpoint.md)
- [教程: 检查点与持久化](../tutorials/checkpointing.md)
