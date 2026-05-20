# Changelog

All notable changes to this project will be documented in this file.

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
