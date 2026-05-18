# 异常类型

所有异常都在 `ZeroGraph.errors` 模块中定义。

## EmptyChannelError

::: zerograph.errors.EmptyChannelError

当从没有值的 Channel 读取数据时抛出。

## InvalidUpdateError

::: zerograph.errors.InvalidUpdateError

当尝试用无效值更新 Channel 时抛出。

## GraphRecursionError

::: zerograph.errors.GraphRecursionError

当图执行超过最大步数限制时抛出。继承自 Python 内置的 `RecursionError`。

## GraphBubbleUp

::: zerograph.errors.GraphBubbleUp

图控制流异常的基类。用于中断（interrupt）和父命令（parent command）等控制流机制。

## GraphInterrupt

::: zerograph.errors.GraphInterrupt

当调用 `interrupt()` 函数时抛出。继承自 `GraphBubbleUp`。

**构造参数**：

| 参数 | 类型 | 说明 |
|------|------|------|
| `interrupts` | `Sequence` | 中断信息列表 |

## ParentCommand

::: zerograph.errors.ParentCommand

当 `Command` 的目标为父图时抛出。继承自 `GraphBubbleUp`。

**构造参数**：

| 参数 | 类型 | 说明 |
|------|------|------|
| `command` | `Command` | 要传递给父图的命令 |
