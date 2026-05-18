# 核心类型

## Send

::: zerograph.types.Send
    options:
      members:
        - __init__

## Command

::: zerograph.types.Command

## Interrupt

::: zerograph.types.Interrupt
    options:
      members:
        - __init__
        - from_ns

## interrupt()

::: zerograph.types.interrupt

## Overwrite

::: zerograph.types.Overwrite

## RetryPolicy

::: zerograph.types.RetryPolicy

字段说明：

| 字段 | 类型 | 默认值 | 说明 |
|------|------|--------|------|
| `initial_interval` | `float` | `0.5` | 首次重试等待时间（秒） |
| `backoff_factor` | `float` | `2.0` | 退避倍数 |
| `max_interval` | `float` | `128.0` | 最大等待时间 |
| `max_attempts` | `int` | `3` | 最大重试次数 |
| `jitter` | `bool` | `True` | 是否添加随机抖动 |
| `retry_on` | `type` / `Sequence` / `Callable` | `Exception` | 触发重试的异常类型 |

## TimeoutPolicy

::: zerograph.types.TimeoutPolicy

字段说明：

| 字段 | 类型 | 默认值 | 说明 |
|------|------|--------|------|
| `run_timeout` | `float` | `None` | 单次执行超时时间（秒） |
| `idle_timeout` | `float` | `None` | 空闲超时时间（秒） |

## StateSnapshot

::: zerograph.types.StateSnapshot

字段说明：

| 字段 | 类型 | 说明 |
|------|------|------|
| `values` | `dict` | 当前状态值 |
| `next` | `tuple[str, ...]` | 下一步要执行的节点 |
| `config` | `dict` | 执行配置 |
| `metadata` | `dict` | 元数据（source, step） |
| `created_at` | `str` | 创建时间 |
| `parent_config` | `dict` | 父配置 |
| `tasks` | `tuple` | 任务列表 |
| `interrupts` | `tuple` | 中断信息 |
| `subgraphs` | `dict` | 子图状态 |

## PregelTask

::: zerograph.types.PregelTask

字段说明：

| 字段 | 类型 | 说明 |
|------|------|------|
| `id` | `str` | 任务 ID |
| `name` | `str` | 节点名 |
| `path` | `tuple` | 执行路径 |
| `error` | `Exception` | 错误信息 |
| `interrupts` | `tuple` | 中断信息 |
| `state` | `Any` | `None` | 任务状态 |
| `result` | `Any` | `None` | 执行结果 |

## All

```python
All = Literal["*"]
```

通配类型，用于表示"所有节点"或"所有通道"的通配符。主要用于内部路由和配置。

## StreamWriter

```python
StreamWriter = Callable[[Any], None]
```

流式写入回调的类型别名。节点内通过 `config["configurable"]["__writer__"]` 获取的 writer 函数即为此类型。
