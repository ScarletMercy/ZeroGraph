# 缓存与 Store

本教程介绍 ZeroGraph 的缓存系统和 Store 系统。

## 缓存系统

缓存系统为节点执行结果提供 TTL 缓存，避免重复计算。

### 基本用法

```python
from zerograph import StateGraph, START, END, InMemoryCache, CachePolicy

# 创建缓存实例
cache = InMemoryCache()

# 编译时传入缓存
app = graph.compile(cache=cache)

# 为特定节点配置缓存策略
graph.add_node(
    "expensive",
    expensive_fn,
    cache_policy=CachePolicy(ttl=60.0)  # 缓存 60 秒
)
```

### CachePolicy

```python
from zerograph import CachePolicy

# TTL 缓存：结果在 60 秒后过期
policy = CachePolicy(ttl=60.0)

# 自定义缓存键
policy = CachePolicy(
    key_func=lambda node_name, inputs: f"{node_name}:{hash(str(inputs))}",
    ttl=120.0
)
```

| 参数 | 类型 | 说明 |
|------|------|------|
| `key_func` | `(str, Any) -> str` | 自定义缓存键生成函数 |
| `ttl` | `float` | 缓存有效期（秒） |

### InMemoryCache

```python
from zerograph import InMemoryCache

cache = InMemoryCache()

# 直接操作
cache.set("key1", "value1", ttl=30.0)
result = cache.get("key1")  # 'value1'
cache.clear()               # 清除所有缓存
```

### 缓存的工作机制

1. 节点配置了 `cache_policy` 后，执行前先检查缓存
2. 缓存命中 → 直接返回缓存值，跳过节点执行
3. 缓存未命中 → 执行节点，将结果写入缓存
4. TTL 到期后缓存自动失效

!!! note "缓存键"
    默认缓存键基于节点名和输入值。如果需要更精细的控制，使用 `key_func` 自定义。

## Store 系统

Store 提供跨调用的键值存储，支持命名空间隔离。

### 基本用法

```python
from zerograph import StateGraph, START, END, InMemoryStore

store = InMemoryStore()

# 编译时传入
app = graph.compile(store=store)
```

### CRUD 操作

```python
store = InMemoryStore()

# 写入
store.put("users", "alice", {"age": 30})
store.put("users", "bob", {"age": 25})

# 读取
item = store.get("users", "alice")
print(item.value)  # {'age': 30}

# 搜索（按前缀）
items = store.search("users", prefix="a", limit=10)

# 删除
store.delete("users", "alice")
```

### StoreItem

```python
from zerograph import StoreItem

item = StoreItem(
    key="alice",
    value={"age": 30},
    namespace="users"
)
print(item.created_at)   # 自动填充的时间戳
print(item.updated_at)   # 自动填充的时间戳
```

| 字段 | 类型 | 说明 |
|------|------|------|
| `key` | `str` | 键名 |
| `value` | `Any` | 值 |
| `namespace` | `str` | 命名空间 |
| `created_at` | `str` | 创建时间（ISO 格式） |
| `updated_at` | `str` | 更新时间（ISO 格式） |

### 在节点中使用 Store

Store 通过 `compile(store=...)` 传入后，节点内可通过 `config` 参数访问：

```python
from zerograph import StateGraph, START, END, InMemoryStore

store = InMemoryStore()
store.put("config", "model", "gpt-4")

def my_node(state: dict, config: dict) -> dict:
    store = config.get("configurable", {}).get("__store__")
    item = store.get("config", "model")
    return {"model": item.value}

graph = StateGraph(dict)
graph.add_node("run", my_node)
graph.add_edge(START, "run")
graph.add_edge("run", END)

app = graph.compile(store=store)
result = app.invoke({})
print(result)  # {'model': 'gpt-4'}
```

## 缓存 + Store 结合使用

```python
from zerograph import StateGraph, START, END, InMemoryCache, InMemoryStore, CachePolicy

cache = InMemoryCache()
store = InMemoryStore()

# 预加载一些数据
store.put("api", "endpoint", "https://api.example.com")

def fetch(state: dict, config: dict) -> dict:
    store = config.get("configurable", {}).get("__store__")
    endpoint = store.get("api", "endpoint").value
    return {"data": f"从 {endpoint} 获取的数据"}

graph = StateGraph(dict)
graph.add_node("fetch", fetch, cache_policy=CachePolicy(ttl=30.0))
graph.add_edge(START, "fetch")
graph.add_edge("fetch", END)

app = graph.compile(cache=cache, store=store)
```

## BaseCache 和 BaseStore

如果需要自定义后端，可以继承抽象基类：

### 自定义缓存

```python
from zerograph import BaseCache

class RedisCache(BaseCache):
    def get(self, key: str):
        # 实现 Redis 读取
        ...

    def set(self, key: str, value, ttl=None):
        # 实现 Redis 写入
        ...

    def clear(self):
        # 实现清空
        ...
```

### 自定义 Store

```python
from zerograph import BaseStore, StoreItem

class PostgresStore(BaseStore):
    def get(self, namespace: str, key: str):
        ...

    def search(self, namespace: str, *, prefix="", limit=10):
        ...

    def put(self, namespace: str, key: str, value):
        ...

    def delete(self, namespace: str, key: str):
        ...
```

## 下一步

- [错误处理与重试](error-handling.md) — 处理节点执行失败
- [函数式 API](functional-api.md) — 在 entrypoint 中使用 store
