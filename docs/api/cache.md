# 缓存系统

## CachePolicy

::: zerograph.cache.CachePolicy

字段说明：

| 字段 | 类型 | 默认值 | 说明 |
|------|------|--------|------|
| `key_func` | `Callable[[str, Any], str]` | `None` | 自定义缓存键生成函数 |
| `ttl` | `float` | `None` | 缓存有效期（秒） |

## BaseCache

::: zerograph.cache.BaseCache
    options:
      members:
        - get
        - set
        - clear

## InMemoryCache

::: zerograph.cache.InMemoryCache
    options:
      members:
        - __init__
        - get
        - set
        - clear
