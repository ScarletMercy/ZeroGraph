# 错误处理与重试

本教程介绍如何在 ZeroGraph 中处理节点执行失败，配置重试策略和超时。

## RetryPolicy：重试策略

`RetryPolicy` 为节点配置自动重试，使用指数退避：

```python
from zerograph import StateGraph, START, END, RetryPolicy

def unreliable_api(state: dict) -> dict:
    # 可能失败的 API 调用
    ...

graph = StateGraph(dict)
graph.add_node(
    "api_call",
    unreliable_api,
    retry_policy=RetryPolicy(
        max_attempts=3,
        initial_interval=0.5,
        backoff_factor=2.0,
        jitter=True,
    )
)
```

### RetryPolicy 参数

| 参数 | 类型 | 默认值 | 说明 |
|------|------|--------|------|
| `initial_interval` | `float` | `0.5` | 首次重试等待时间（秒） |
| `backoff_factor` | `float` | `2.0` | 退避倍数（每次等待 × backoff_factor） |
| `max_interval` | `float` | `128.0` | 最大等待时间上限 |
| `max_attempts` | `int` | `3` | 最大重试次数 |
| `jitter` | `bool` | `True` | 是否添加随机抖动 |
| `retry_on` | `type` / `Sequence` / `Callable` | `Exception` | 触发重试的异常类型 |

### 重试行为示例

```
第 1 次失败 → 等待 0.5s
第 2 次失败 → 等待 1.0s
第 3 次失败 → 等待 2.0s
第 3 次仍失败 → 抛出异常
```

### 自定义重试条件

```python
# 只重试特定的异常
RetryPolicy(retry_on=ConnectionError)

# 重试多种异常
RetryPolicy(retry_on=[ConnectionError, TimeoutError])

# 使用函数判断
def should_retry(exc: Exception) -> bool:
    return isinstance(exc, ValueError) and "retry" in str(exc)

RetryPolicy(retry_on=should_retry)
```

## TimeoutPolicy：超时策略

`TimeoutPolicy` 限制节点的执行时间：

```python
from zerograph import StateGraph, START, END, TimeoutPolicy

graph.add_node(
    "slow_task",
    slow_function,
    timeout=TimeoutPolicy(run_timeout=10.0)
)
```

### TimeoutPolicy 参数

| 参数 | 类型 | 说明 |
|------|------|------|
| `run_timeout` | `float` | 单次执行超时时间（秒） |
| `idle_timeout` | `float` | 空闲超时时间（秒） |

### 快捷方式

也可以直接传数字或 `timedelta`：

```python
graph.add_node("task", fn, timeout=10.0)  # 等价于 TimeoutPolicy(run_timeout=10.0)

from datetime import timedelta
graph.add_node("task", fn, timeout=timedelta(seconds=30))
```

## error_handler：错误路由

可以为节点指定一个专门的错误处理节点：

```python
def may_fail(state: dict) -> dict:
    if state["x"] < 0:
        raise ValueError("x 不能为负数")
    return {"result": state["x"] * 2}

def handle_error(state: dict) -> dict:
    error = state.get("__error__")
    return {"result": f"错误: {error}"}

graph = StateGraph(dict)
graph.add_node("process", may_fail, error_handler="on_error")
graph.add_node("on_error", handle_error)
graph.add_edge(START, "process")
graph.add_edge("process", END)
graph.add_edge("on_error", END)

app = graph.compile()
```

当 `process` 节点抛出异常时：

1. 异常被捕获，`__error__` 字段设置为错误信息
2. 执行路由到 `on_error` 节点
3. `on_error` 可以访问 `__error__` 获取错误详情

!!! note "error_handler 与 retry 的关系"
    重试先于错误路由。如果重试次数耗尽后仍然失败，才会路由到 error_handler 节点。

## set_node_defaults：批量设置策略

一次性为所有已有节点设置默认策略：

```python
graph = StateGraph(dict)
graph.add_node("a", fn_a)
graph.add_node("b", fn_b)
graph.add_node("c", fn_c)

graph.set_node_defaults(
    retry_policy=RetryPolicy(max_attempts=5),
    timeout=TimeoutPolicy(run_timeout=30.0),
    error_handler="error_handler",
)
```

!!! note "只覆盖空值"
    `set_node_defaults` 只覆盖节点当前为 `None` 的设置。如果某个节点已经配置了 `retry_policy`，不会被覆盖。

## GraphRecursionError

当图执行超过最大步数时，抛出 `GraphRecursionError`：

```python
from zerograph import GraphRecursionError

try:
    result = app.invoke({"x": 0})
except GraphRecursionError:
    print("图执行超出了最大步数限制")
```

## 自定义异常类型

ZeroGraph 定义了完整的异常层次结构：

| 异常 | 继承自 | 触发场景 |
|------|--------|---------|
| `EmptyChannelError` | `Exception` | 从空 Channel 读取 |
| `InvalidUpdateError` | `Exception` | 无效的 Channel 更新 |
| `GraphRecursionError` | `RecursionError` | 超出最大步数 |
| `GraphBubbleUp` | `Exception` | 控制流基类 |
| `GraphInterrupt` | `GraphBubbleUp` | 中断执行 |
| `ParentCommand` | `GraphBubbleUp` | Command 目标为父图 |

## 完整示例：健壮的 API 调用图

```python
from zerograph import StateGraph, START, END, RetryPolicy, TimeoutPolicy

def call_api(state: dict) -> dict:
    # 模拟可能失败的 API 调用
    import random
    if random.random() < 0.3:
        raise ConnectionError("API 不可用")
    return {"result": "success"}

def on_error(state: dict) -> dict:
    return {"result": f"降级处理: {state.get('__error__', '未知错误')}"}

def finalize(state: dict) -> dict:
    return {"final": state.get("result", "无结果")}

graph = StateGraph(dict)
graph.add_node(
    "api",
    call_api,
    retry_policy=RetryPolicy(max_attempts=3, retry_on=ConnectionError),
    timeout=TimeoutPolicy(run_timeout=5.0),
    error_handler="fallback"
)
graph.add_node("fallback", on_error)
graph.add_node("finalize", finalize)
graph.add_edge(START, "api")
graph.add_edge("api", "finalize")
graph.add_edge("fallback", "finalize")
graph.add_edge("finalize", END)

app = graph.compile()
```

## 下一步

- [检查点与持久化](checkpointing.md) — 保存执行状态以便恢复
- [构建 ReAct 智能体](../how-to/react-agent.md) — 构建实际应用
