# Store 系统

## StoreItem

::: zerograph.store.StoreItem

字段说明：

| 字段 | 类型 | 说明 |
|------|------|------|
| `key` | `str` | 键名 |
| `value` | `Any` | 值 |
| `namespace` | `str` | 命名空间 |
| `created_at` | `str` | 创建时间（自动填充） |
| `updated_at` | `str` | 更新时间（自动填充） |

## BaseStore

::: zerograph.store.BaseStore
    options:
      members:
        - get
        - search
        - put
        - delete

## InMemoryStore

::: zerograph.store.InMemoryStore
    options:
      members:
        - __init__
        - get
        - search
        - put
        - delete
