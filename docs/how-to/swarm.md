# 构建群智协作

本指南展示如何使用 `create_swarm` 构建一个基于交接（handoff）的多 Agent 群智系统。

## 目标

构建一个多 Agent 系统，Agent 之间通过 handoff 机制互相交接控制权：

1. 第一个 Agent 接收输入
2. Agent 执行后可以选择 handoff 给另一个 Agent
3. 没有 handoff 时，群组终止

## 最小示例

```python
from zerograph import create_swarm

# Agent 1: 客服
def customer_service(state: dict) -> dict:
    last = state["messages"][-1]["content"] if state["messages"] else ""
    if "退款" in last:
        return {
            "messages": [{"role": "assistant", "content": "我来帮你转接售后部门"}],
            "handoff": "after_sales"  # 交接给售后 Agent
        }
    return {"messages": [{"role": "assistant", "content": "请问有什么可以帮您？"}]}

# Agent 2: 售后
def after_sales(state: dict) -> dict:
    return {
        "messages": [{"role": "assistant", "content": "售后处理中，您的退款将在3天内到账"}]
    }

# 创建群组
swarm = create_swarm([customer_service, after_sales])
result = swarm.invoke({
    "messages": [{"role": "user", "content": "我要退款"}]
})
print(result["messages"][-1]["content"])  # 售后处理中...
```

## handoff 机制

Agent 通过返回的消息字典中的 `"handoff"` 字段来交接控制权：

```python
def agent_a(state: dict) -> dict:
    return {
        "messages": [{"role": "assistant", "content": "转交给 Agent B"}],
        "handoff": "agent_b"  # 值必须是另一个 Agent 的名称
    }
```

### 交接规则

| 返回值 | 行为 |
|--------|------|
| 包含 `handoff` 且值是合法 Agent 名 | 路由到目标 Agent |
| 包含 `handoff` 但 Agent 不存在 | 直接终止 |
| 不包含 `handoff` | 直接终止 |
| 包含 `tool_calls` | 路由到共享工具节点 |

## 共享工具

可以为所有 Agent 配置共享的工具节点：

```python
from zerograph import create_swarm

def search_db(state: dict) -> str:
    """查询数据库。"""
    return "查询结果..."

def customer_service(state: dict) -> dict:
    return {
        "messages": [
            {"role": "assistant", "content": None, "tool_calls": [{
                "id": "call_1",
                "type": "function",
                "function": {"name": "search_db", "arguments": '{"query": "订单状态"}'}
            }]}
        ]
    }

swarm = create_swarm(
    [customer_service, after_sales],
    tools=[search_db]  # 共享工具
)
```

## Agent 命名

Agent 的名称由 `__name__` 属性决定：

```python
def billing(state: dict) -> dict: ...  # 名称: "billing"
def support(state: dict) -> dict: ...  # 名称: "support"

swarm = create_swarm([billing, support])
# billing → handoff="support" → support
```

如果名称冲突，会自动添加序号后缀。

## 配置检查点

```python
from zerograph import create_swarm, InMemorySaver

swarm = create_swarm(
    [customer_service, after_sales],
    checkpointer=InMemorySaver()
)

config = {"configurable": {"thread_id": "swarm-1"}}
result = swarm.invoke({"messages": [{"role": "user", "content": "我要退款"}]}, config)
```

## 完整示例：智能客服群组

```python
from zerograph import create_swarm, InMemorySaver

def greeter(state: dict) -> dict:
    """接待员: 根据用户需求分配到不同部门。"""
    last = state["messages"][-1]["content"] if state["messages"] else ""

    if "退款" in last or "退货" in last:
        return {
            "messages": [{"role": "assistant", "content": "我来帮您转接售后服务"}],
            "handoff": "after_sales"
        }
    elif "技术" in last or "故障" in last:
        return {
            "messages": [{"role": "assistant", "content": "我来帮您转接技术支持"}],
            "handoff": "tech_support"
        }
    return {"messages": [{"role": "assistant", "content": "感谢咨询，再见！"}]}

def after_sales(state: dict) -> dict:
    """售后服务: 处理退款和退货。"""
    return {
        "messages": [{"role": "assistant", "content": "退款已受理，预计3天到账。还有其他需要吗？"}],
        "handoff": "greeter"  # 交回接待员
    }

def tech_support(state: dict) -> dict:
    """技术支持: 处理技术问题。"""
    return {
        "messages": [{"role": "assistant", "content": "已记录您的技术问题，工程师将在24小时内联系您。"}]
    }

swarm = create_swarm(
    [greeter, after_sales, tech_support],
    checkpointer=InMemorySaver()
)

config = {"configurable": {"thread_id": "customer-1"}}
result = swarm.invoke({
    "messages": [{"role": "user", "content": "我买的商品想退款"}]
}, config)
```

## 参考文档

- [API: create_swarm](../api/prebuilt.md)
- [构建多 Agent 监督者](supervisor.md)
- [构建 ReAct 智能体](react-agent.md)
