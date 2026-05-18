# 构建多 Agent 监督者

本指南展示如何使用 `create_supervisor` 构建一个由监督者协调的多 Agent 系统。

## 目标

构建一个多 Agent 系统，监督者（Supervisor）根据任务内容将工作分配给不同的专业 Agent：

1. 用户发送消息
2. 监督者 LLM 分析消息，决定分配给哪个 Agent
3. Agent 执行任务，返回结果
4. 监督者决定是否需要更多工作或结束

## 最小示例

```python
from zerograph import create_supervisor

# Agent 1: 研究员
def researcher(state: dict) -> dict:
    return {"messages": [{"role": "assistant", "content": "研究结果: ..."}]}

# Agent 2: 写作员
def writer(state: dict) -> dict:
    return {"messages": [{"role": "assistant", "content": "写作完成: ..."}]}

# 监督者 LLM
def supervisor_llm(messages, tools=None):
    last_msg = messages[-1]["content"] if messages else ""

    if "研究" in last_msg:
        return {
            "role": "assistant",
            "content": "分配给研究员",
            "tool_calls": [{
                "id": "call_1",
                "type": "function",
                "function": {"name": "researcher", "arguments": '{"input": "研究任务"}'}
            }]
        }

    return {"role": "assistant", "content": "所有工作已完成"}

# 创建监督者
supervisor = create_supervisor(supervisor_llm, [researcher, writer])
result = supervisor.invoke({
    "messages": [{"role": "user", "content": "请帮我研究一下 AI 的发展"}]
})
```

## 工作流程

```
用户消息 → 监督者 → 分析 → 分配给 Agent
                         ↓
              Agent 执行 → 返回监督者 → 分析 → ...
                                           ↓
                                    无需更多工作 → 结束
```

## llm_callable 接口

监督者的 `llm_callable` 需要支持通过 `tool_calls` 路由到不同 Agent：

```python
def supervisor_llm(messages, tools=None):
    """
    Args:
        messages: 消息列表
        tools: Agent 列表的 JSON Schema（自动生成）

    Returns:
        要路由到 Agent: 返回带 tool_calls 的消息，function.name 匹配 Agent 名
        要结束: 返回不带 tool_calls 的消息
    """
```

### Agent 命名

- 如果传入函数，默认使用 `function.__name__`
- 如果传入 `CompiledStateGraph`，使用 `agent_0`, `agent_1` 等
- 自动处理重名：`researcher`, `researcher_1`, `researcher_2`

## 使用 CompiledStateGraph 作为 Agent

```python
from zerograph import StateGraph, START, END, create_supervisor

# 创建一个独立的 Agent 图
research_graph = StateGraph(dict)
research_graph.add_node("search", lambda s: {"data": "搜索结果"})
research_graph.add_node("analyze", lambda s: {"result": f"分析: {s['data']}"})
research_graph.add_edge(START, "search")
research_graph.add_edge("search", "analyze")
research_graph.add_edge("analyze", END)
research_app = research_graph.compile()

# 将编译后的图作为 Agent
supervisor = create_supervisor(supervisor_llm, [research_app, writer])
```

## 配置检查点

```python
from zerograph import create_supervisor, InMemorySaver

supervisor = create_supervisor(
    supervisor_llm,
    [researcher, writer],
    checkpointer=InMemorySaver()
)
```

## 自定义状态

```python
from typing import Annotated, TypedDict
from zerograph import add_messages

class TeamState(TypedDict):
    messages: Annotated[list, add_messages]
    team_name: str

supervisor = create_supervisor(
    supervisor_llm,
    [researcher, writer],
    state_schema=TeamState
)
```

## 完整示例：内容团队

```python
from zerograph import create_supervisor, InMemorySaver

def researcher(state: dict) -> dict:
    topic = state["messages"][-1]["content"]
    return {
        "messages": [{
            "role": "assistant",
            "content": f"关于 '{topic}' 的研究数据: [数据1, 数据2, 数据3]"
        }]
    }

def writer(state: dict) -> dict:
    research = state["messages"][-1]["content"]
    return {
        "messages": [{
            "role": "assistant",
            "content": f"基于研究数据撰写的文章: 《AI 发展报告》"
        }]
    }

def supervisor_llm(messages, tools=None):
    user_msg = messages[0]["content"] if messages else ""
    last_msg = messages[-1]["content"] if messages else ""

    # 如果是用户消息且包含研究需求
    if "研究" in user_msg and not any("研究数据" in m.get("content", "") for m in messages):
        return {
            "role": "assistant",
            "content": "先研究",
            "tool_calls": [{
                "id": "call_1",
                "type": "function",
                "function": {"name": "researcher", "arguments": '{"input": "research"}'}
            }]
        }

    # 如果有研究结果，分配给写作员
    if "研究数据" in last_msg:
        return {
            "role": "assistant",
            "content": "分配写作",
            "tool_calls": [{
                "id": "call_2",
                "type": "function",
                "function": {"name": "writer", "arguments": '{"input": "write"}'}
            }]
        }

    return {"role": "assistant", "content": "内容团队任务完成！"}

team = create_supervisor(
    supervisor_llm,
    [researcher, writer],
    checkpointer=InMemorySaver()
)

config = {"configurable": {"thread_id": "team-1"}}
result = team.invoke({
    "messages": [{"role": "user", "content": "请研究 AI 并写一篇文章"}]
}, config)
```

## 参考文档

- [API: create_supervisor](../api/prebuilt.md)
- [构建 ReAct 智能体](react-agent.md)
- [构建群智协作](swarm.md)
