# 函数式 API

本教程介绍 `@entrypoint` 和 `@task` 装饰器——用函数而非图来定义工作流。

## 核心概念

函数式 API 提供了一种更直观的方式来定义工作流：

- `@task` — 标记一个函数为独立工作单元（延迟执行）
- `@entrypoint` — 标记一个函数为工作流入口

与 `StateGraph` 的区别：

| 特性 | StateGraph | 函数式 API |
|------|-----------|-----------|
| 定义方式 | 声明式（节点+边） | 命令式（函数调用） |
| 适合场景 | 复杂状态机、条件路由 | 线性管道、数据处理 |
| 流式支持 | 完整 | 基础（writer + 最终结果） |

## @task：延迟执行

`@task` 装饰的函数不会立即执行，而是返回一个 `_TaskFuture` 对象：

```python
from zerograph import task

@task
def fetch_data(url: str) -> dict:
    return {"data": f"来自 {url} 的数据"}

@task
def process(data: dict) -> dict:
    return {"result": data["data"].upper()}

# 调用时返回 Future，不立即执行
future = fetch_data("https://example.com")
print(future)  # <_TaskFuture ...>

# 调用 .result() 才执行
result = future.result()
print(result)  # {'data': '来自 https://example.com 的数据'}
```

## @entrypoint：工作流入口

`@entrypoint` 将普通函数包装为可执行的工作流：

```python
from zerograph import entrypoint, task

@task
def step1(x: int) -> int:
    return x + 1

@task
def step2(x: int) -> int:
    return x * 2

@entrypoint()
def workflow(inp, config=None):
    a = step1(inp["value"])
    b = step2(a.result())
    return b.result()

# 执行工作流
result = workflow.invoke({"value": 5})
print(result)  # 12（(5+1)*2）
```

## previous 参数：跨调用记忆

配置 checkpointer 后，可以在函数签名中添加 `previous` 参数获取上一次的返回值：

```python
from zerograph import entrypoint, InMemorySaver

@entrypoint(checkpointer=InMemorySaver())
def counter(inp, *, previous=None, config=None):
    prev_count = previous if previous is not None else 0
    return prev_count + 1

config = {"configurable": {"thread_id": "1"}}
print(counter.invoke({}, config))  # 1
print(counter.invoke({}, config))  # 2
print(counter.invoke({}, config))  # 3
```

## store 参数

添加 `store` 参数可以访问 Store 系统：

```python
from zerograph import entrypoint, InMemoryStore

@entrypoint(store=InMemoryStore())
def workflow(inp, *, store=None, config=None):
    store.put("cache", "last_input", inp)
    return {"done": True}
```

## writer 参数：自定义流式事件

函数签名中包含 `writer` 时，可以发送自定义流式事件：

```python
@entrypoint()
def pipeline(inp, *, writer=None, config=None):
    writer("步骤 1 完成")
    writer("步骤 2 完成")
    return {"status": "done"}

for event in pipeline.stream({"x": 1}):
    print(event)
# {'entrypoint:events': ['步骤 1 完成', '步骤 2 完成']}
# {'entrypoint': {'status': 'done'}}
```

## 异步执行

```python
@entrypoint()
async def async_workflow(inp, config=None):
    a = await step1(inp["value"]).aresult()
    b = await step2(a).aresult()
    return b

result = await async_workflow.ainvoke({"value": 5})
```

## stream 与 astream

```python
# 同步流式
for event in workflow.stream({"value": 5}):
    print(event)

# 异步流式
async for event in workflow.astream({"value": 5}):
    print(event)
```

支持的 `stream_mode`：

| 模式 | 产出 |
|------|------|
| `"updates"`（默认） | writer 事件 + 最终结果 |
| `"custom"` | 仅 writer 事件 |

## 完整示例：数据处理管道

```python
from zerograph import entrypoint, task, InMemorySaver

@task
def extract(text: str) -> list[str]:
    return text.split(",")

@task
def transform(items: list[str]) -> list[str]:
    return [item.strip().upper() for item in items]

@task
def load(items: list[str]) -> dict:
    return {"count": len(items), "items": items}

@entrypoint(checkpointer=InMemorySaver())
def etl_pipeline(inp, *, previous=None, writer=None, config=None):
    raw = inp.get("data", "")
    tokens = extract(raw)
    cleaned = transform(tokens.result())
    result = load(cleaned.result())
    return result.result()

config = {"configurable": {"thread_id": "etl-1"}}

result = etl_pipeline.invoke({"data": "foo, bar, baz"}, config)
print(result)  # {'count': 3, 'items': ['FOO', 'BAR', 'BAZ']}

# previous 保存了上次的返回值
result2 = etl_pipeline.invoke({"data": "hello, world"}, config)
```

## 下一步

- [缓存与 Store](cache-and-store.md) — 深入了解缓存和 Store
- [错误处理与重试](error-handling.md) — 为 task 配置重试策略
