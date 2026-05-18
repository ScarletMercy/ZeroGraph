# ZeroGraph

轻量级图执行引擎 — 零外部依赖，纯 Python 实现。

ZeroGraph 实现了类 Pregel 的超步执行模型，支持检查点、流式输出、子图嵌套和预构建的 LLM Agent 模式。

## 特性

- **:material-sitemap: StateGraph** — 将工作流定义为类型化的状态机，支持节点、边和条件路由
- **:material-database: Channel 系统** — 通过 Reducer 灵活管理状态（`LastValue`、`BinaryOperatorAggregate`、`AnyValue` 等）
- **:material-content-save: 检查点** — 使用 `InMemorySaver` 和 `SqliteSaver` 持久化并恢复执行
- **:material-lightning-bolt: 流式输出** — 支持 `values`、`updates`、`custom`、`messages`、`checkpoints`、`tasks` 六种模式
- **:material-source-branch: 子图** — 图中嵌套图，状态命名空间隔离
- **:material-pause: 中断与恢复** — 在任意节点暂停执行，稍后恢复并传入用户输入
- **:material-function-variant: 函数式 API** — `@entrypoint` 和 `@task` 装饰器定义工作流
- **:material-robot: 预构建 Agent** — `ToolNode`、`create_react_agent`、`create_supervisor`、`create_swarm`
- **:material-eye: 可视化** — 从任意 StateGraph 生成 Mermaid 流程图

## 安装

```bash
pip install zerograph
```

## 30 秒示例

```python
from zerograph import StateGraph, START, END

graph = StateGraph(dict)
graph.add_node("hello", lambda s: {"msg": "Hello!"})
graph.add_node("bye", lambda s: {"msg": "Goodbye!"})
graph.add_edge(START, "hello")
graph.add_edge("hello", "bye")
graph.add_edge("bye", END)

app = graph.compile()
print(app.invoke({}))  # {'msg': 'Goodbye!'}
```

## 导航

<div class="grid cards" markdown>

- :rocket: **[快速上手](getting-started.md)** — 5 分钟学会基础用法
- :book: **[教程](tutorials/basic-graph.md)** — 按主题逐步深入每个特性
- :tools: **[操作指南](how-to/react-agent.md)** — 解决具体问题的完整代码
- :fontawesome-brands-python: **[API 参考](api/index.md)** — 所有公共接口的详细文档

</div>
