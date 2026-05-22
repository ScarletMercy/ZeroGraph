"""ZeroGraph - A lightweight graph execution engine with zero external dependencies."""

from zerograph.constants import START, END, TAG_HIDDEN
from zerograph.errors import (
    EmptyChannelError,
    GraphBubbleUp,
    GraphInterrupt,
    GraphRecursionError,
    InvalidUpdateError,
    ParentCommand,
)
from zerograph.graph import StateGraph, CompiledStateGraph
from zerograph.checkpoint import (
    BaseCheckpointSaver,
    InMemorySaver,
    Checkpoint,
    CheckpointMetadata,
    CheckpointTuple,
)
from zerograph.types import (
    All,
    Command,
    Interrupt,
    Overwrite,
    PregelTask,
    RetryPolicy,
    Send,
    StateSnapshot,
    TimeoutPolicy,
    interrupt,
)
from zerograph.channels.messages import add_messages, RemoveMessage
from zerograph.channels.any_value import AnyValue
from zerograph.channels.base import BaseChannel
from zerograph.channels.last_value import LastValue
from zerograph.channels.binop import BinaryOperatorAggregate
from zerograph.channels.topic import Topic
from zerograph.channels.ephemeral_value import EphemeralValue
from zerograph.channels.named_barrier import NamedBarrierValue
from zerograph.cache import BaseCache, InMemoryCache, CachePolicy
from zerograph.store import BaseStore, InMemoryStore, StoreItem
from zerograph.func import entrypoint, task

try:
    from zerograph.checkpoint.sqlite import SqliteSaver, AsyncSqliteSaver
except ImportError:
    SqliteSaver = None  # type: ignore[assignment,misc]
    AsyncSqliteSaver = None  # type: ignore[assignment,misc]

try:
    from zerograph.prebuilt import ToolNode, create_react_agent, create_supervisor, create_swarm
    from zerograph.prebuilt.tool_node import InjectedState, InjectedStore
except ImportError:
    ToolNode = None  # type: ignore[assignment,misc]
    create_react_agent = None  # type: ignore[assignment,misc]
    create_supervisor = None  # type: ignore[assignment,misc]
    create_swarm = None  # type: ignore[assignment,misc]
    InjectedState = None  # type: ignore[assignment,misc]
    InjectedStore = None  # type: ignore[assignment,misc]

try:
    from zerograph.adapters import LLMStreamAdapter
except ImportError:
    LLMStreamAdapter = None  # type: ignore[assignment,misc]

__all__ = (
    # Constants
    "START",
    "END",
    "TAG_HIDDEN",
    # Graph
    "StateGraph",
    "CompiledStateGraph",
    # Checkpoint
    "BaseCheckpointSaver",
    "InMemorySaver",
    "Checkpoint",
    "CheckpointMetadata",
    "CheckpointTuple",
    # Types
    "All",
    "Command",
    "Interrupt",
    "Overwrite",
    "PregelTask",
    "RetryPolicy",
    "Send",
    "StateSnapshot",
    "TimeoutPolicy",
    "interrupt",
    # Errors
    "EmptyChannelError",
    "GraphBubbleUp",
    "GraphInterrupt",
    "GraphRecursionError",
    "InvalidUpdateError",
    "ParentCommand",
    # Messages
    "add_messages",
    "RemoveMessage",
    # Channels
    "BaseChannel",
    "AnyValue",
    "LastValue",
    "BinaryOperatorAggregate",
    "Topic",
    "EphemeralValue",
    "NamedBarrierValue",
    # Cache
    "BaseCache",
    "InMemoryCache",
    "CachePolicy",
    # Store
    "BaseStore",
    "InMemoryStore",
    "StoreItem",
    # Functional API
    "entrypoint",
    "task",
    # SQLite Checkpoint
    "SqliteSaver",
    "AsyncSqliteSaver",
    # Prebuilt
    "ToolNode",
    "InjectedState",
    "InjectedStore",
    "create_react_agent",
    "create_supervisor",
    "create_swarm",
    # Adapters
    "LLMStreamAdapter",
)
