# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.1.0] - 2025-05-18

### Added

- StateGraph with typed state, reducers, conditional edges, and waiting edges (fan-in).
- Channel system: `LastValue`, `BinaryOperatorAggregate`, `AnyValue`, `EphemeralValue`, `Topic`, `NamedBarrierValue`.
- Checkpoint system: `InMemorySaver`, `SqliteSaver`, `AsyncSqliteSaver` with thread-safe WAL mode.
- Streaming with multiple modes: `values`, `updates`, `custom`, `messages`, `checkpoints`, `tasks`.
- Subgraph support with namespace-isolated state and checkpoint hierarchy.
- Interrupt and resume with checkpoint persistence.
- Functional API: `@entrypoint` and `@task` decorators.
- Cache system with TTL support (`InMemoryCache`).
- Store system for long-term key-value memory (`InMemoryStore`).
- Prebuilt components: `ToolNode`, `create_react_agent`, `create_supervisor`, `create_swarm`.
- `LLMStreamAdapter` for OpenAI and Anthropic chunk streaming.
- Mermaid diagram visualization (`get_mermaid`).
- `add_messages` reducer with upsert-by-ID and `RemoveMessage` support.
- `RetryPolicy` with exponential backoff and jitter.
- `TimeoutPolicy` for per-node timeout configuration.
- `Command` for state update and flow control (`goto`, `PARENT`).
- Error handler routing with `set_error_handler`.
- Parallel async execution with semaphore-based concurrency control.
- Batch execution (`batch`, `abatch`).
- State history via `get_state_history`.
- State mutation via `update_state` with `as_node` support.
