# Changelog

All notable changes to this project will be documented in this file.

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
