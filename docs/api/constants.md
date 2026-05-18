# 常量

## 公共常量

以下常量在 `ZeroGraph.constants` 中定义，并通过 `ZeroGraph` 包直接导出。

### START

```python
START = "__start__"
```

图的入口节点名称。所有从 START 出发的边定义了图的起始节点。

```python
graph.add_edge(START, "first_node")
```

### END

```python
END = "__end__"
```

图的终止节点名称。所有指向 END 的边表示执行到此结束。

```python
graph.add_edge("last_node", END)
```

### TAG_HIDDEN

```python
TAG_HIDDEN = "langsmith:hidden"
```

用于标记隐藏节点的元数据标签（兼容 LangSmith）。

## 内部常量

以下是 ZeroGraph 内部使用的常量，通常不需要直接使用：

| 常量 | 值 | 用途 |
|------|-----|------|
| `TASKS` | `"__tasks__"` | 任务通道 |
| `INTERRUPT` | `"__interrupt__"` | 中断通道 |
| `RESUME` | `"__resume__"` | 恢复通道 |
| `ERROR` | `"__error__"` | 错误通道 |
| `PREVIOUS` | `"__previous__"` | 前一次调用 |
| `NS_SEP` | `"\|"` | 命名空间分隔符 |
| `NS_END` | `":"` | 命名空间终止符 |
| `NULL_TASK_ID` | `""` | 空任务 ID |
