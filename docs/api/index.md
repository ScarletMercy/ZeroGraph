# API 参考总览

ZeroGraph 的所有公共接口按功能分类如下。

## 核心图构建

| 符号 | 说明 |
|------|------|
| [`StateGraph`](graph.md#zerograph.graph.state.StateGraph) | 图构建器，定义节点、边和状态模式 |
| [`CompiledStateGraph`](graph.md#zerograph.graph.state.CompiledStateGraph) | 编译后的可执行图 |
| [`START`](constants.md) | 入口常量 `"__start__"` |
| [`END`](constants.md) | 终止常量 `"__end__"` |
| [`TAG_HIDDEN`](constants.md) | 隐藏节点标记（兼容 LangSmith） |

## 核心类型

| 符号 | 说明 |
|------|------|
| [`Send`](types.md#zerograph.types.Send) | 动态发送数据到指定节点 |
| [`Command`](types.md#zerograph.types.Command) | 同时更新状态和控制流 |
| [`Interrupt`](types.md#zerograph.types.Interrupt) | 中断信息对象 |
| [`interrupt()`](types.md#zerograph.types.interrupt) | 在节点内触发中断 |
| [`RetryPolicy`](types.md#zerograph.types.RetryPolicy) | 重试策略配置 |
| [`TimeoutPolicy`](types.md#zerograph.types.TimeoutPolicy) | 超时策略配置 |
| [`StateSnapshot`](types.md#zerograph.types.StateSnapshot) | 状态快照 |
| [`PregelTask`](types.md#zerograph.types.PregelTask) | 任务执行信息 |
| [`Overwrite`](types.md#zerograph.types.Overwrite) | 绕过 Reducer 直接覆盖 |
| [`All`](types.md) | 通配类型 `Literal["*"]` |
| [`StreamWriter`](types.md) | 流式写入回调类型 |

## Channel 系统

| 符号 | 说明 |
|------|------|
| [`BaseChannel`](channels.md) | Channel 抽象基类 |
| [`LastValue`](channels.md) | 最后值覆盖 |
| [`BinaryOperatorAggregate`](channels.md) | Reducer 聚合 |
| [`AnyValue`](channels.md) | 任意值（多次写入不报错） |
| [`add_messages`](channels.md) | 消息列表 Reducer |
| [`RemoveMessage`](channels.md) | 消息删除标记 |

## 检查点

| 符号 | 说明 |
|------|------|
| [`BaseCheckpointSaver`](checkpoint.md) | 检查点存储抽象基类 |
| [`InMemorySaver`](checkpoint.md) | 内存检查点存储 |
| [`SqliteSaver`](checkpoint.md) | SQLite 检查点存储 |
| [`AsyncSqliteSaver`](checkpoint.md) | 异步 SQLite 检查点存储 |
| [`Checkpoint`](checkpoint.md) | 检查点数据结构（TypedDict） |
| [`CheckpointMetadata`](checkpoint.md) | 检查点元数据（TypedDict） |
| [`CheckpointTuple`](checkpoint.md) | 检查点元组（NamedTuple） |

## 预构建组件

| 符号 | 说明 |
|------|------|
| [`ToolNode`](prebuilt.md) | 自动执行工具调用的节点 |
| [`InjectedState`](prebuilt.md) | 注入图状态到工具参数 |
| [`InjectedStore`](prebuilt.md) | 注入 Store 到工具参数 |
| [`create_react_agent`](prebuilt.md) | 一行创建 ReAct 智能体 |
| [`create_supervisor`](prebuilt.md) | 创建多 Agent 监督者 |
| [`create_swarm`](prebuilt.md) | 创建群智协作系统 |

## 函数式 API

| 符号 | 说明 |
|------|------|
| [`entrypoint`](func.md) | 工作流入口装饰器 |
| [`task`](func.md) | 独立工作单元装饰器 |

## 基础设施

| 符号 | 说明 |
|------|------|
| [`BaseCache`](cache.md) / [`InMemoryCache`](cache.md) | 缓存系统 |
| [`CachePolicy`](cache.md) | 缓存策略配置 |
| [`BaseStore`](store.md) / [`InMemoryStore`](store.md) | 键值存储系统 |
| [`StoreItem`](store.md) | 存储项数据类 |
| [`LLMStreamAdapter`](adapters.md) | LLM 流式适配器 |

## 异常

| 符号 | 说明 |
|------|------|
| [`EmptyChannelError`](errors.md) | 从空 Channel 读取 |
| [`InvalidUpdateError`](errors.md) | 无效 Channel 更新 |
| [`GraphRecursionError`](errors.md) | 超出最大步数 |
| [`GraphInterrupt`](errors.md) | 图中断 |
| [`GraphBubbleUp`](errors.md) | 控制流基类 |
| [`ParentCommand`](errors.md) | 父图命令 |
