# Changelog

All notable changes to this project will be documented in this file.

## [0.5.0] - 2026-05-23

### Added
- 支持 `InMemorySaver` 全方法线程安全锁和 `__deepcopy__`
- 支持 SQLite JSON 编码格式版本化（`__zg_v1_type__` + `__zg_v1_v__`），兼容旧格式
- 支持 `SqliteSaver` 关闭后防重复使用（`_closed` 标记）
- 支持 send 节点中 `GraphInterrupt` 正确保存中断并返回
- 支持异步 generator 检测（返回时抛出 `TypeError`）
- 支持 `_apply_input` 处理 `Overwrite` 值
- 支持超时执行继承上下文变量（`contextvars.copy_context()`）
- 支持异步重试带超时（`asyncio.wait_for` + 重试循环）
- 支持工具参数为 dict 时跳过 JSON 解析
- 支持 `_inject_args` 复制参数字典防突变
- 新增 `tests/test_channels.py` 通道测试
- 新增 `tests/test_interrupt.py` 中断测试

### Fixed
- 修复 `_TaskFuture` 自旋等待改为 `asyncio.Event` 通知
- 修复 `_TaskFuture.result()` 未检测异步函数（协程泄漏）
- 修复任务执行后未释放参数内存（`_func`/`_args`/`_kwargs` 置 `None`）
- 修复入口点 `_build_kwargs` 未跳过首参注入
- 修复 pending writes 在用户输入之后应用（应为之前，用户输入优先）
- 修复 `GraphBubbleUp` 被重试策略吞掉（现在直接重新抛出）
- 修复 SQLite `Interrupt` 序列化仅在 `INTERRUPT` 通道触发
- 修复 `AsyncSqliteSaver.close()` 使用 `wait=False` 导致资源泄漏
- 修复 `Topic.checkpoint()` 冗余 `if self.values` 检查
- 修复缓存键使用 `hash(repr())`（改为 SHA256 避免碰撞）
- 修复可视化标签未转义换行/花括号/尖括号
- 修复可视化 `md5` 改为 `sha256`
- 修复可视化数字开头 ID 缺少前缀

### Removed
- 删除 `pregel/_algo.py` 390 行死代码
- 移除 `pregel/__init__.py` 中已删除模块的导出

### Changed
- `_deepcopy_or_warn` 日志级别从 debug 提升为 warning
- SQLite 导入改为可选（`checkpoint/__init__.py`）

## [0.4.0] - 2026-05-22

### Added
- 支持可选依赖导入（sqlite、prebuilt、adapters），缺失时优雅降级
- 支持统一的 `_deepcopy_or_warn()` 工具函数，失败时回退到原始引用
- 支持 `InMemoryCache` LRU 淘汰策略（OrderedDict + popitem）
- 支持线程安全锁（`InMemoryCache`、`InMemoryStore`、`_TaskFuture`）
- 支持 `_collect_goto` / `_extract_routing` 辅助函数，消除重复路由逻辑
- 支持 `StateGraph`、`CompiledStateGraph`、`PregelLoop` 的 `__repr__`
- 支持 `invoke()` / `stream()` 异步函数守卫（异步函数调用时抛出 `TypeError`）
- 支持 `ToolNode.invoke()` 异步工具守卫
- 支持生成器/异步生成器异常时正确关闭（`.close()`）
- 支持 `Send.timeout` 单次发送超时覆盖（优先于节点默认超时）
- 支持 `_internal`、`func`、`pregel._loop` 日志模块
- 支持并行中断收集（所有中断统一收集后返回）
- 支持 `_get_start_nodes` 中 `Command.goto` 路由处理
- 支持 `Topic._flatten()` 中 tuple 类型

### Fixed
- 修复 `react_agent` 迭代次数 off-by-one 错误（`>` → `>=`）
- 修复 swarm handoff 无限循环（仅接受当前 agent 自身消息的 handoff）
- 修复 `add_messages` 中 `updated_by_new` 未正确跟踪替换/删除
- 修复 `ContextVar` 重置未在 `finally` 块中执行（并行执行资源泄漏）
- 修复递归限制检查顺序（先检查 `next_nodes` 为空再抛出 `RecursionError`）
- 修复并行中断处理（中断存在时抑制非中断错误）
- 修复 batch/abatch（每个输入获得独立 config 副本，防止交叉污染）
- 修复 `_read_output` 单通道返回 `{}`（现在返回 `None`）
- 修复 `_process_result`（仅在字典非空时应用更新）
- 修复 `Interrupt.__hash__`（移除不可哈希的 `value`）
- 修复 `NamedBarrierValue.checkpoint()`（空 `seen` 集合返回 `MISSING`）
- 修复 `channel_values` checkpoint（不再重复深拷贝）
- 修复 `ThreadPoolExecutor.shutdown`（添加 `cancel_futures=True`）
- 修复 `_call_node`（处理协程返回值和同步函数中的生成器返回值）
- 修复 `BinaryOperatorAggregate.__eq__`（添加身份短路优化）

### Changed
- `InMemoryCache` 改用 `OrderedDict` LRU 淘汰（原为普通 dict FIFO）
- 提取 `_collect_goto` / `_extract_routing`，减少约 150 行重复代码
- 改进 `_extract_schema` 类型提示解析（使用 `get_type_hints()`）
- 改进 `_TaskFuture.aresult()`（基于锁的并发协调）
- 改进错误聚合（所有错误拼接而非仅保留最后一个）

## [0.3.0] - 2026-05-21

### Added
- 支持 Command.goto 路由（Send/str/list），统一节点结果路由逻辑
- 支持 waiting_edges（fan-in 边），目标节点仅在前置节点全部完成后触发
- 支持 resume 时保留 step 计数，不重置为 0
- 支持 interrupt 保存多个中断值（_save_interrupts）
- 支持子图自动命名（state_schema 名称 + 去重计数器）
- 支持 swarm/supervisor 保留名冲突避免
- 支持 tuple 类型的 conditional_edges 返回值
- 支持 namedtuple 在 _resolve_futures 中正确还原
- 支持 frozenset 在 _resolve_futures 中处理
- 支持 Python 3.10+ UnionType（X | Y）在工具 schema 推断中
- 支持 timedelta 序列化/反序列化
- 支持 datetime 使用 _ZG_TYPE 结构化编码（而非裸 isoformat）
- 支持编译时验证节点目标是已知节点
- 支持 tool_node tuple 类型映射

### Fixed
- 修复子图 _loop 交换竞态条件（移除 _subgraph_lock，使用 copy 替代原地修改）
- 修复 _read_node_input 返回部分状态（现在任一 channel 不可用则抛 EmptyChannelError）
- 修复 interrupt 时 _current_config 未在 finally 中 reset（使用 finally 确保清理）
- 修复 interrupt checkpoint 只保存当前节点（现在保留所有待执行节点）
- 修复 send 节点结果中 Command.goto 未被处理（统一处理 Send/Command.goto）
- 修复 _compute_updates 不处理 Command.update（现在从 Command 提取 update）
- 修复 _get_next_nodes 对 list 结果重复路由（shortcut_path 避免重复调用 router）
- 修复 parallel 路径 new_next 被覆盖而非 update（result_info["new_next"].update）
- 修复 parallel 路径异常类型只捕获 Exception（改为 BaseException）
- 修复 send_inputs 未在 send 节点完成后清理（添加 send_inputs.pop）
- 修复 GraphInterrupt 可变默认参数（() → None + 条件赋值）
- 修复 Send.__hash__/__eq__ 不包含 timeout 字段
- 修复 add_messages 中 update 与 remove 冲突（updated_by_new 集合）
- 修复 NamedBarrierValueAfterFinish.update 不处理空值序列
- 修复 _decode_obj 缺少 data 键时崩溃（所有类型添加 data 检查）
- 修复 error_val 缺少 error_type 字段（统一添加）
- 修复 error_handler 中 input read failed 不包含实际错误信息
- 修复 tool_node 不支持 tuple 类型映射

## [0.2.0] - 2026-05-20

### Fixed (112 bugs)

#### Critical
- 修复 `_safe_copy` 浅拷贝问题，防止 checkpoint 数据损坏
- 修复 SQLite JSON 序列化类型丢失（tuple/frozenset/set/bytes）
- 修复 checkpoint 输入突变导致的状态污染
- 修复 asyncio.run 语义问题，防止事件循环冲突
- 修复线程池重复创建导致的资源泄漏
- 修复并行写入竞态条件，实现两阶段提交
- 修复 AsyncSqliteSaver `:memory:` 模式同步方法创建独立 DB
- 修复 SQLite 连接泄漏
- 修复魔法键冲突（`__zerograph_type__` 结构）
- 修复子图执行锁保护范围，防止并发状态损坏
- 修复 `_TaskFuture` 异常重新抛出
- 修复 generator 节点在并行模式返回 generator 对象
- 修复 async 子图锁阻塞事件循环
- 修复 falsy checkpoint ID 注入

#### High
- 修复 supervisor 变量顺序依赖导致的不可预测行为
- 修复 LLM adapter 流式响应工具调用索引错误
- 修复 TAG_HIDDEN 耦合问题，支持独立隐藏节点
- 修复缺失的导出和项目 URL
- 修复 Send 批次循环深度限制错误
- 修复 tool_node 参数注入异常捕获范围
- 修复 state.py 异常处理过宽问题
- 修复 config 深拷贝性能问题
- 修复 SQLite 编码器对不可序列化类型的处理
- 修复 InMemoryCache/InMemorySaver 无大小限制导致的内存泄漏
- 修复共享引用问题（channels 深拷贝）
- 修复 binop.py 计数器逻辑和 Overwrite 值处理
- 修复 RemoveMessage 过滤同批次新增消息
- 修复 Command.update falsy 值被静默丢弃
- 修复 pending_writes 深拷贝和线程安全
- 修复 `_resolve_futures` 不支持 set 类型
- 修复工具 schema 包含 `*args`/`**kwargs`
- 修复 `messages[-1]` 无类型检查导致的 AttributeError

#### Medium
- 修复 `_read_node_input` 返回部分状态
- 修复 `Scratchpad` 可变默认参数
- 修复 channels 浅拷贝 fallback（topic/last_value/ephemeral/any_value）
- 修复 `binop.checkpoint()` 返回可变引用
- 修复 `get_pending_writes` 返回内部列表引用
- 修复 `close()` 只清除调用线程的 local 连接
- 修复 `list()` 的 `before` 过滤行为不一致
- 修复并行路径缺少 debug 流事件
- 修复 `_resolve_futures` 不处理 set/frozenset
- 修复 interrupt 数据损坏时 KeyError
- 修复 AsyncSqliteSaver close() 顺序

#### Low
- 修复 `empty.key` 冗余赋值
- 修复 binop `from_checkpoint` TypeError 绕过
- 修复 sqlite `_setup_conn` 失败泄漏
- 修复 frozenset 类型丢失
- 修复 `seen_overwrite` 死代码
- 修复文档字符串错误

### Changed
- 移动 `_MAX_SEND_DEPTH` 到 `constants.py`
- 拆分 `_process_result` 为 `_compute_updates` + `_apply_updates`
- 优化 `_safe_copy` 降级策略：deepcopy → copy → original

### Added
- 添加 `_get_callable_name` 辅助函数
- 添加 `Union`/`Optional` 类型处理
- 添加 `error_type` 到错误字典

### Testing
- 152 个测试全部通过

## [0.1.0] - 2026-05-XX

### Added
- 初始版本发布
- 核心图执行引擎
- 状态管理和检查点系统
- 流式输出支持
- 多智能体编排
- SQLite 和内存检查点存储
- 预构建组件（React Agent、Tool Node、Supervisor、Swarm）
