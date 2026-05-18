# LLM 流式适配

本指南展示如何使用 `LLMStreamAdapter` 和 `stream_openai` 适配 OpenAI 和 Anthropic 的流式输出。

## 目标

将 LLM SDK 的流式 chunk 转换为 ZeroGraph 可处理的消息格式，支持：

- 文本增量流式输出
- 工具调用的增量参数组装
- 同时兼容 OpenAI 和 Anthropic 格式

## LLMStreamAdapter

`LLMStreamAdapter` 是一个无状态的累积器，逐步接收 chunk 并组装最终消息。

### 非流式用法

```python
from zerograph import LLMStreamAdapter

adapter = LLMStreamAdapter()

# 模拟 OpenAI chunk
class Chunk:
    def __init__(self, delta):
        self.choices = [type("Choice", (), {"delta": delta})]

adapter.append(Chunk(delta=type("Delta", (), {"content": "你好"})), provider="openai")
adapter.append(Chunk(delta=type("Delta", (), {"content": "世界"})), provider="openai")

msg = adapter.build_message()
print(msg)  # {'role': 'assistant', 'content': '你好世界'}
```

### 流式用法（配合 generator 节点）

!!! note "外部依赖"
    本示例使用 OpenAI Python SDK。需要 `pip install openai` 并配置 API 密钥。

```python
from zerograph import StateGraph, START, END, LLMStreamAdapter
from openai import OpenAI

openai_client = OpenAI()  # 从环境变量 OPENAI_API_KEY 读取密钥

def streaming_llm(state: dict):
    adapter = LLMStreamAdapter()
    for chunk in openai_client.chat.completions.create(
        model="gpt-4",
        messages=state["messages"],
        stream=True,
    ):
        delta = adapter.append(chunk, provider="openai")
        if delta:
            yield {"role": "assistant", "content": delta}
    return {"messages": [adapter.build_message()]}

graph = StateGraph(dict)
graph.add_node("llm", streaming_llm)
graph.add_edge(START, "llm")
graph.add_edge("llm", END)

app = graph.compile()
for event in app.stream({"messages": [{"role": "user", "content": "Hello"}]}, stream_mode="messages"):
    print(event)
```

## append 方法

```python
adapter.append(chunk, provider="openai")  # 返回 str | None
```

| 参数 | 说明 |
|------|------|
| `chunk` | SDK 返回的 chunk 对象（鸭子类型，不需要导入 SDK） |
| `provider` | `"openai"` 或 `"anthropic"` |

返回值是本次 chunk 的文本增量（`str` 或 `None`）。

## build_message 方法

```python
msg = adapter.build_message()
```

返回完整消息字典：

- 纯文本：`{"role": "assistant", "content": "..."}`
- 工具调用：`{"role": "assistant", "content": "...", "tool_calls": [...]}`

## OpenAI 格式

```python
adapter = LLMStreamAdapter()

# 文本 chunk
adapter.append(openai_text_chunk, provider="openai")

# 工具调用 chunk（增量参数）
adapter.append(openai_tool_start_chunk, provider="openai")  # function.name
adapter.append(openai_tool_args_chunk, provider="openai")    # function.arguments 增量
```

`LLMStreamAdapter` 会自动将增量的 `arguments` 字符串拼接成完整的 JSON。

## Anthropic 格式

```python
adapter = LLMStreamAdapter()

# 文本 chunk（content_block_delta）
adapter.append(anthropic_text_chunk, provider="anthropic")

# 工具调用 chunk（content_block_start + input_json_delta）
adapter.append(anthropic_tool_start_chunk, provider="anthropic")
adapter.append(anthropic_tool_input_chunk, provider="anthropic")
```

## stream_openai 便捷函数

`stream_openai` 是一个生成器函数，封装了常见的 OpenAI 流式调用模式：

```python
from zerograph.adapters.llm_stream import stream_openai

# 调用 LLM 并流式获取文本
state_update = None
for text_delta in stream_openai(
    llm_callable,                    # 接受 **kwargs 的调用函数
    messages=[{"role": "user", "content": "Hello"}],
    tools=tools_schema,              # 可选
    stream=True,
):
    print(text_delta, end="")        # 逐字输出

# 生成器返回值是完整的状态更新
state_update = ...  # 使用 yield + return 模式
```

### 使用方式

```python
def llm_callable(**kwargs):
    return openai_client.chat.completions.create(**kwargs)

gen = stream_openai(
    llm_callable,
    messages=[{"role": "user", "content": "讲个笑话"}],
    stream=True,
)

for delta in gen:
    print(delta, end="")

# 获取最终状态更新（通过 send + yield 返回值模式）
```

## 鸭子类型

`LLMStreamAdapter` 不依赖任何 SDK，通过鸭子类型访问 chunk 属性：

**OpenAI chunk 需要的属性**：
- `chunk.choices[0].delta.content` — 文本内容
- `chunk.choices[0].delta.tool_calls[i].function.name` — 工具名
- `chunk.choices[0].delta.tool_calls[i].function.arguments` — 工具参数

**Anthropic chunk 需要的属性**：
- `chunk.type == "content_block_delta"` + `chunk.delta.text` — 文本
- `chunk.type == "content_block_start"` + `chunk.content_block` — 工具开始
- `chunk.type == "input_json_delta"` + `chunk.partial_json` — 工具参数

## 参考文档

- [API: LLMStreamAdapter](../api/adapters.md)
- [教程: 流式输出](../tutorials/streaming.md)
