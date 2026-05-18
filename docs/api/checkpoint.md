# 检查点系统

## BaseCheckpointSaver

::: zerograph.checkpoint.base.BaseCheckpointSaver
    options:
      members:
        - get_tuple
        - get
        - put
        - put_writes
        - list
        - delete_thread

## Checkpoint 类型

::: zerograph.checkpoint.base.Checkpoint

::: zerograph.checkpoint.base.CheckpointMetadata

::: zerograph.checkpoint.base.CheckpointTuple

## InMemorySaver

::: zerograph.checkpoint.memory.InMemorySaver
    options:
      members:
        - get_tuple
        - put
        - put_writes
        - list
        - delete_thread
        - get_pending_writes

## SqliteSaver

::: zerograph.checkpoint.sqlite.SqliteSaver
    options:
      members:
        - __init__
        - get_tuple
        - put
        - put_writes
        - list
        - delete_thread
        - get_pending_writes
        - close

## AsyncSqliteSaver

::: zerograph.checkpoint.sqlite.AsyncSqliteSaver
    options:
      members:
        - __init__
        - aget_tuple
        - aput
        - aput_writes
        - alist
        - adelete_thread
        - aget_pending_writes
        - close
