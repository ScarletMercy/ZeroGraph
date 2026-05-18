# 构建 ReAct 智能体

本指南展示如何使用 `create_react_agent` 一行代码构建一个完整的 ReAct 智能体。

## 目标

构建一个能调用工具的智能体，工作流程：

1. LLM 分析用户消息，决定是否调用工具
2. 如需调用工具 → 执行工具 → 将结果反馈给 LLM
3. 重复直到 LLM 认为不需要更多工具调用

## 最小示例

```python
from zerograph import create_react_agent

# 定义工具
def search(query: str) -> str:
    """搜索互联网获取信息。"""
    return f"搜索结果: {query} 的相关信息..."

def calculator(expression: str) -> float:
    """计算数学表达式。"""
    return eval(expression)

# 模拟 LLM 调用
def my_llm(messages, tools=None):
    last = messages[-1]["content"] if messages else ""

    # 第一次调用：决定使用计算器
    if "2+2" in last and not any(m.get("tool_calls") for m in messages):
        return {
            "role": "assistant",
            "content": None,
            "tool_calls": [{
                "id": "call_1",
                "type": "function",
                "function": {
                    "name": "calculator",
                    "arguments": '{"expression": "2+2"}'
                }
            }]
        }

    # 第二次调用：总结结果
    tool_results = [m for m in messages if m.get("role") == "tool"]
    if tool_results:
        return {
            "role": "assistant",
            "content": f"计算结果是 {tool_results[-1]['content']}"
        }

    return {"role": "assistant", "content": "你好！有什么可以帮你？"}

# 一行创建 Agent
agent = create_react_agent(my_llm, [search, calculator])

# 执行
result = agent.invoke({
    "messages": [{"role": "user", "content": "请计算 2+2"}]
})
print(result["messages"][-1]["content"])  # 计算结果是 4
```

## llm_callable 接口

`llm_callable` 是你提供的 LLM 调用函数，签名如下：

```python
def my_llm(messages: list[dict], tools: list[dict] | None = None) -> dict:
    """
    Args:
        messages: 消息列表，每条消息是 {"role": "...", "content": "...", ...}
        tools: OpenAI 格式的工具 schema 列表

    Returns:
        消息字典，必须包含 "role" 和 "content"。
        如果要调用工具，还需包含 "tool_calls" 字段。
    """
```

### 返回值格式

**不调用工具**：
```python
{"role": "assistant", "content": "回答内容"}
```

**调用工具**：
```python
{
    "role": "assistant",
    "content": None,
    "tool_calls": [{
        "id": "call_1",
        "type": "function",
        "function": {"name": "tool_name", "arguments": '{"arg1": "value1"}'}
    }]
}
```

## 工具定义

工具就是普通 Python 函数：

```python
def get_weather(city: str) -> str:
    """获取指定城市的天气信息。"""
    return f"{city}: 晴, 25°C"

def send_email(to: str, subject: str, body: str) -> str:
    """发送电子邮件。"""
    return f"邮件已发送至 {to}"
```

ZeroGraph 自动从函数签名和 docstring 生成 JSON Schema：

- 函数名 → `function.name`
- docstring → `function.description`
- 参数名和类型注解 → `function.parameters`

## 配置检查点

```python
from zerograph import create_react_agent, InMemorySaver

agent = create_react_agent(
    my_llm,
    [search, calculator],
    checkpointer=InMemorySaver()
)

config = {"configurable": {"thread_id": "session-1"}}

# 第一轮对话
result = agent.invoke({"messages": [{"role": "user", "content": "你好"}]}, config)

# 第二轮对话（保留上下文）
result = agent.invoke({"messages": [{"role": "user", "content": "刚才我问了什么？"}]}, config)
```

## max_iterations

限制 Agent 的最大推理轮数，防止无限循环：

```python
agent = create_react_agent(my_llm, tools, max_iterations=10)
```

默认值是 `25`。

## 自定义状态模式

```python
from typing import Annotated, TypedDict
from zerograph import create_react_agent, add_messages

class MyState(TypedDict):
    messages: Annotated[list, add_messages]
    user_id: str

agent = create_react_agent(
    my_llm,
    tools,
    state_schema=MyState
)
```

## 流式执行

```python
for event in agent.stream({"messages": [{"role": "user", "content": "你好"}]}):
    print(event)
# {'agent': {'messages': [...]}}
# {'tools': {'messages': [...]}}
# {'agent': {'messages': [...]}}
```

## 参考文档

- [API: create_react_agent](../api/prebuilt.md)
- [API: ToolNode](../api/prebuilt.md)
- [教程: 检查点与持久化](../tutorials/checkpointing.md)
