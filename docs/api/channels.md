# Channel 系统

Channel 是 ZeroGraph 状态管理的核心机制。每个状态字段对应一个 Channel 实例，负责管理该字段的读写和合并逻辑。

## BaseChannel

::: zerograph.channels.base.BaseChannel
    options:
      members:
        - __init__
        - ValueType
        - UpdateType
        - get
        - update
        - from_checkpoint
        - checkpoint
        - copy
        - is_available
        - consume
        - finish

## 内置 Channel 类型

### Channel 选择指南

| Channel 类型 | 适用场景 | 典型用法 |
|-------------|---------|---------|
| `LastValue` | 默认行为，每个 key 只保留最后一个值 | 简单状态字段 |
| `BinaryOperatorAggregate` | 需要累加/合并多个节点的输出 | `Annotated[list, operator.add]`、自定义 reducer |
| `AnyValue` | 允许多次写入同一 key（不报错） | `context_schema` 注入、内部机制 |
| `EphemeralValue` | 值只在一个超步中有效，之后自动清空 | 一次性信号、临时标记 |
| `Topic` | 发布/订阅模式，可累积消息列表 | 广播事件、消息总线 |
| `NamedBarrierValue` | 等待所有指定来源都写入后才可读 | fan-in 同步、等待多路汇聚 |

### LastValue

::: zerograph.channels.last_value.LastValue
    options:
      members:
        - __init__
        - update
        - get

### BinaryOperatorAggregate

::: zerograph.channels.binop.BinaryOperatorAggregate
    options:
      members:
        - __init__
        - update
        - get

### AnyValue

::: zerograph.channels.any_value.AnyValue
    options:
      members:
        - __init__
        - update
        - get

### EphemeralValue

::: zerograph.channels.ephemeral_value.EphemeralValue
    options:
      members:
        - __init__
        - update
        - get

### Topic

::: zerograph.channels.topic.Topic
    options:
      members:
        - __init__
        - update
        - get
        - consume

### NamedBarrierValue

::: zerograph.channels.named_barrier.NamedBarrierValue
    options:
      members:
        - __init__
        - update
        - get

## 消息 Channel

### add_messages

::: zerograph.channels.messages.add_messages

### RemoveMessage

::: zerograph.channels.messages.RemoveMessage
    options:
      members:
        - __init__
