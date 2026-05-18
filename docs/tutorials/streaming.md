# 流式输出

本教程介绍 ZeroGraph 的 6 种流式输出模式及其组合使用。

## 基本用法

`stream()` 方法返回一个生成器，按指定模式产出事件：

```python
for event in app.stream(input_data, stream_mode="updates"):
    print(event)
```

## 六种流式模式

### 1. `updates`（默认）

每次节点执行后，产出该节点的输出更新：

```python
for event in app.stream({"x": 0}, stream_mode="updates"):
    print(event)
# {'step1': {'x': 1}}
# {'step2': {'x': 2}}
```

### 2. `values`

每步完成后产出完整状态快照：

```python
for event in app.stream({"count": 0}, stream_mode="values"):
    print(event)
# {'count': 0}
# {'count': 1}
# {'count': 3}
```

### 3. `custom`

节点内通过 `writer` 函数发送自定义事件。`writer` 通过 `config` 参数注入：

```python
def my_node(state: dict, config: dict) -> dict:
    writer = config.get("configurable", {}).get("__writer__")
    writer("进度 50%")
    writer("进度 100%")
    return {"done": True}

for event in app.stream({"done": False}, stream_mode="custom"):
    print(event)
# 进度 50%
# 进度 100%
```

!!! tip "writer 获取方式"
    在 StateGraph 节点中，`writer` 需要通过 `config["configurable"]["__writer__"]` 获取。`@entrypoint` 函数式 API 中可以直接在签名里声明 `writer` 参数。

### 4. `messages`

专为消息流设计，节点内通过 `yield` 产出消息片段：

```python
def streaming_node(state: dict):
    yield {"role": "assistant", "content": "你"}
    yield {"role": "assistant", "content": "好"}
    return {"text": "你好"}

for event in app.stream({}, stream_mode="messages"):
    print(event)
```

### 5. `checkpoints`

每次检查点保存时产出检查点信息（需要配置 checkpointer）：

```python
from zerograph import InMemorySaver

app = graph.compile(checkpointer=InMemorySaver())
config = {"configurable": {"thread_id": "1"}}

for event in app.stream({"x": 0}, config=config, stream_mode="checkpoints"):
    print(event)  # Checkpoint 对象
```

### 6. `tasks`

每个任务（节点执行）开始和结束时产出任务信息：

```python
for event in app.stream({"x": 0}, stream_mode="tasks"):
    print(event)  # PregelTask 对象
```

## 多模式组合

可以同时使用多种模式，传入列表即可：

```python
for event in app.stream(
    {"x": 0},
    config=config,
    stream_mode=["values", "updates"]
):
    print(event)
# 产出的每个事件会是元组: (mode_name, data)
```

示例输出：

```
('values', {'x': 0})
('updates', {'step1': {'x': 1}})
('values', {'x': 1})
('updates', {'step2': {'x': 2}})
('values', {'x': 2})
```

## 异步流式

```python
async for event in app.astream({"x": 0}, stream_mode="updates"):
    print(event)
```

## debug 模式

使用 `stream_mode="debug"` 可以获得更详细的执行日志：

```python
app = graph.compile()
for event in app.stream({"x": 0}, stream_mode="debug"):
    print(event)
```

## 下一步

- [检查点与持久化](checkpointing.md) — 保存执行状态
- [LLM 流式适配](../how-to/llm-streaming.md) — 适配 OpenAI/Anthropic 流式输出
