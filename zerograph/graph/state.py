"""StateGraph and CompiledStateGraph - the main user-facing graph builder."""

from __future__ import annotations

import asyncio
import inspect
from collections import defaultdict
from collections.abc import Awaitable, Callable, Hashable, Sequence
from dataclasses import dataclass
from datetime import timedelta
from functools import partial
from inspect import isfunction, ismethod, signature
from typing import (
    Annotated,
    Any,
    Generic,
    Literal,
    TypeVar,
    Union,
    cast,
    get_args,
    get_origin,
    get_type_hints,
)

from zerograph._internal import EMPTY_SEQ, MISSING
from zerograph.channels.base import BaseChannel
from zerograph.channels.binop import BinaryOperatorAggregate
from zerograph.channels.ephemeral_value import EphemeralValue
from zerograph.channels.last_value import LastValue
from zerograph.checkpoint.base import BaseCheckpointSaver
from zerograph.constants import END, START
from zerograph.pregel._loop import PregelLoop
from zerograph.types import (
    Command,
    Interrupt,
    RetryPolicy,
    Send,
    TimeoutPolicy,
)

__all__ = ("StateGraph", "CompiledStateGraph")

StateT = TypeVar("StateT")
InputT = TypeVar("InputT")
OutputT = TypeVar("OutputT")

_CHANNEL_BRANCH_TO = "branch:to:{}"


@dataclass
class BranchSpec:
    path: Callable
    ends: dict | None = None

    def run(self, writer):
        """Create a routing runnable for this branch."""
        def route(input, config):
            value = self.path(input)
            if not isinstance(value, (list, tuple)):
                value = [value]
            destinations = []
            for r in value:
                if isinstance(r, Send):
                    destinations.append(r)
                elif self.ends and r in self.ends:
                    destinations.append(self.ends[r])
                elif r == END:
                    pass
                else:
                    destinations.append(r)
            entries = writer(destinations, False)
            return entries
        return route


@dataclass
class NodeSpec:
    runnable: Callable
    metadata: dict | None = None
    input_schema: type | None = None
    retry_policy: RetryPolicy | None = None
    ends: tuple | dict | None = EMPTY_SEQ
    defer: bool = False
    timeout: TimeoutPolicy | None = None
    error_handler_node: str | None = None
    is_error_handler: bool = False
    cache_policy: Any | None = None


class StateGraph(Generic[StateT, InputT, OutputT]):
    """A graph whose nodes communicate by reading and writing to shared state.

    Each node has signature ``State -> Partial[State]``.
    State keys can be annotated with reducer functions via
    ``Annotated[type, reducer]``.

    Usage::

        from typing import Annotated
        import operator
        from ZeroGraph import StateGraph, START, END

        class MyState(TypedDict):
            messages: Annotated[list, operator.add]

        graph = StateGraph(MyState)
        graph.add_node("greet", lambda s: {"messages": ["Hello"]})
        graph.add_edge(START, "greet")
        graph.add_edge("greet", END)

        app = graph.compile()
        result = app.invoke({"messages": []})

    Args:
        state_schema: TypedDict or dict type defining the graph state.
        input_schema: Optional separate schema for graph input. Defaults to state_schema.
        output_schema: Optional separate schema for graph output. Defaults to state_schema.
        context_schema: Optional schema for immutable context values injected into nodes.
    """

    def __init__(
        self,
        state_schema: type,
        *,
        input_schema: type | None = None,
        output_schema: type | None = None,
        context_schema: type | None = None,
    ) -> None:
        self.nodes: dict[str, NodeSpec] = {}
        self.edges: set[tuple[str, str]] = set()
        self.branches: dict[str, dict[str, BranchSpec]] = defaultdict(dict)
        self.channels: dict[str, BaseChannel] = {}
        self.schemas: dict[type, dict[str, BaseChannel]] = {}
        self.waiting_edges: set[tuple[tuple[str, ...], str]] = set()
        self.compiled = False

        self.state_schema = state_schema
        self.input_schema = input_schema or state_schema
        self.output_schema = output_schema or state_schema
        self.context_schema = context_schema

        self._add_schema(self.state_schema)
        self._add_schema(self.input_schema)
        self._add_schema(self.output_schema)

        if context_schema is not None:
            self._add_context_schema(context_schema)

    @property
    def _all_edges(self) -> set[tuple[str, str]]:
        return self.edges | {
            (start, end) for starts, end in self.waiting_edges for start in starts
        }

    def _add_schema(self, schema: type) -> None:
        if schema not in self.schemas:
            channels = _get_channels(schema)
            self.schemas[schema] = channels
            for key, channel in channels.items():
                if key in self.channels:
                    if isinstance(channel, LastValue):
                        continue
                    if self.channels[key] != channel:
                        raise ValueError(
                            f"Channel '{key}' already exists with a different type"
                        )
                else:
                    self.channels[key] = channel

    def _add_context_schema(self, schema: type) -> None:
        from zerograph.channels.any_value import AnyValue
        if not hasattr(schema, "__annotations__"):
            return
        type_hints = get_type_hints(schema)
        context_channels = {}
        for key, typ in type_hints.items():
            if key == "__slots__":
                continue
            if key in self.channels:
                raise ValueError(
                    f"context_schema key '{key}' conflicts with state_schema key"
                )
            ch = AnyValue(typ, key)
            ch.key = key
            self.channels[key] = ch
            context_channels[key] = ch
        self.schemas[schema] = context_channels

    def add_node(
        self,
        node: str | Callable,
        action: Callable | None = None,
        *,
        metadata: dict | None = None,
        input_schema: type | None = None,
        retry_policy: RetryPolicy | None = None,
        destinations: dict[str, str] | tuple[str, ...] | None = None,
        timeout: float | timedelta | TimeoutPolicy | None = None,
        cache_policy: Any | None = None,
        error_handler: str | None = None,
    ) -> StateGraph:
        """Add a node to the graph.

        The action function receives the current state and returns a dict
        of updates to merge.  A ``CompiledStateGraph`` can be passed as
        ``action`` to embed a subgraph.

        Args:
            node: Node name (str) or a callable (uses ``__name__``).
            action: The function to execute.  If *node* is callable this is
                set automatically.
            metadata: Optional metadata dict attached to the node.
            input_schema: Override the input schema for this node.
            retry_policy: Automatic retry configuration.
            destinations: Pre-declared routing targets for ``Command.goto``.
            timeout: Timeout in seconds, ``timedelta``, or ``TimeoutPolicy``.
            cache_policy: Cache configuration for this node's results.
            error_handler: Name of another node to route to on exception.

        Returns:
            self, for method chaining.
        """
        # Handle subgraph: CompiledStateGraph as action
        subgraph = None
        if not isinstance(node, str) and isinstance(node, CompiledStateGraph):
            subgraph = node
            action = node
            node = "subgraph"
        elif isinstance(action, CompiledStateGraph):
            subgraph = action
        elif not isinstance(node, str):
            action = node
            node = getattr(action, "__name__", action.__class__.__name__)

        if action is None:
            raise RuntimeError("Action must be provided")
        if node in self.nodes:
            raise ValueError(f"Node `{node}` already present.")
        if node in (END, START):
            raise ValueError(f"Node `{node}` is reserved.")

        if isinstance(timeout, (int, float)):
            timeout = TimeoutPolicy(run_timeout=float(timeout))
        elif isinstance(timeout, timedelta):
            timeout = TimeoutPolicy(run_timeout=timeout.total_seconds())

        inferred_input = None
        ends: tuple | dict = EMPTY_SEQ

        # Subgraph: infer input schema from subgraph's builder
        if subgraph is not None and input_schema is None:
            inferred_input = subgraph.builder.state_schema

        # Infer input schema and destinations from function annotations
        try:
            if isfunction(action) or ismethod(action) or callable(action):
                hints = get_type_hints(getattr(action, "__call__", action))
                sig = signature(getattr(action, "__call__", action))
                params = list(sig.parameters.keys())
                if params and input_schema is None:
                    first_param = params[0]
                    if first_param in hints:
                        hint = hints[first_param]
                        if isinstance(hint, type) and hasattr(hint, "__annotations__"):
                            inferred_input = hint

                rtn = hints.get("return")
                if rtn:
                    rtn_origin = get_origin(rtn)
                    if rtn_origin is Union:
                        for arg in get_args(rtn):
                            arg_origin = get_origin(arg)
                            if arg_origin is Command:
                                rtn = arg
                                rtn_origin = arg_origin
                                break
                    if (
                        rtn_origin is Command
                        and (rargs := get_args(rtn))
                        and get_origin(rargs[0]) is Literal
                        and (vals := get_args(rargs[0]))
                    ):
                        ends = vals
        except (NameError, TypeError, StopIteration):
            pass

        if destinations is not None:
            ends = destinations

        resolved_input = input_schema or inferred_input or self.state_schema

        self.nodes[node] = NodeSpec(
            runnable=action,
            metadata=metadata,
            input_schema=resolved_input,
            retry_policy=retry_policy,
            ends=ends,
            defer=False,
            timeout=timeout,
            cache_policy=cache_policy,
            error_handler_node=error_handler,
        )

        if input_schema is not None:
            self._add_schema(input_schema)
        elif inferred_input is not None:
            self._add_schema(inferred_input)

        return self

    def add_edge(self, start_key: str | list[str], end_key: str) -> StateGraph:
        """Add a directed edge.

        Args:
            start_key: Source node name, START, or a list of source nodes
                (creates a waiting / fan-in edge).
            end_key: Target node name or END.

        Returns:
            self, for method chaining.
        """
        if isinstance(start_key, str):
            if start_key == END:
                raise ValueError("END cannot be a start node")
            if end_key == START:
                raise ValueError("START cannot be an end node")
            if start_key != START and start_key not in self.nodes:
                raise ValueError(f"Need to add_node `{start_key}` first")
            if end_key != END and end_key not in self.nodes:
                raise ValueError(f"Need to add_node `{end_key}` first")
            self.edges.add((start_key, end_key))
            return self

        for start in start_key:
            if start == END:
                raise ValueError("END cannot be a start node")
            if start not in self.nodes:
                raise ValueError(f"Need to add_node `{start}` first")
        if end_key == START:
            raise ValueError("START cannot be an end node")
        if end_key != END and end_key not in self.nodes:
            raise ValueError(f"Need to add_node `{end_key}` first")

        self.waiting_edges.add((tuple(start_key), end_key))
        return self

    def add_conditional_edges(
        self,
        source: str,
        path: Callable,
        path_map: dict[Hashable, str] | list[str] | None = None,
    ) -> StateGraph:
        """Add a conditional edge from source node.

        Args:
            source: Source node name.
            path: Routing function ``(state) -> str | list[str | Send]``.
            path_map: Maps return values of *path* to node names.

        Returns:
            self, for method chaining.
        """
        path_map_: dict | None = None
        if isinstance(path_map, dict):
            path_map_ = path_map.copy()
        elif isinstance(path_map, list):
            path_map_ = {name: name for name in path_map}
        else:
            try:
                hints = get_type_hints(path)
                rtn = hints.get("return")
                if rtn and get_origin(rtn) is Literal:
                    path_map_ = {name: name for name in get_args(rtn)}
            except Exception:
                pass

        name = getattr(path, "__name__", "condition")
        if name in self.branches[source]:
            raise ValueError(
                f"Branch with name `{name}` already exists for node `{source}`"
            )
        self.branches[source][name] = BranchSpec(path=path, ends=path_map_)
        return self

    def add_sequence(
        self,
        nodes: Sequence[Callable | tuple[str, Callable]],
    ) -> StateGraph:
        """Add a sequence of nodes with edges between them.

        Args:
            nodes: Sequence of callables or ``(name, callable)`` tuples.

        Returns:
            self, for method chaining.
        """
        if len(nodes) < 1:
            raise ValueError("Sequence requires at least one node.")

        previous_name: str | None = None
        for i, node in enumerate(nodes):
            if isinstance(node, tuple) and len(node) == 2:
                name, node = node
            else:
                name = getattr(node, "__name__", node.__class__.__name__)

            # Auto-deduplicate names
            base_name = name
            counter = 1
            while name in self.nodes:
                name = f"{base_name}_{counter}"
                counter += 1

            self.add_node(name, node)
            if previous_name is not None:
                self.add_edge(previous_name, name)
            previous_name = name

        return self

    def set_node_defaults(
        self,
        *,
        retry_policy: RetryPolicy | None = None,
        timeout: float | timedelta | TimeoutPolicy | None = None,
        cache_policy: Any | None = None,
        error_handler: str | None = None,
    ) -> StateGraph:
        """Set default policies for all existing nodes.

        Only overwrites a node's setting if the node currently has the default
        (``None``) value for that field.
        """
        for spec in self.nodes.values():
            if retry_policy is not None and spec.retry_policy is None:
                spec.retry_policy = retry_policy
            if timeout is not None and spec.timeout is None:
                if isinstance(timeout, (int, float)):
                    spec.timeout = TimeoutPolicy(run_timeout=float(timeout))
                elif isinstance(timeout, timedelta):
                    spec.timeout = TimeoutPolicy(run_timeout=timeout.total_seconds())
                else:
                    spec.timeout = timeout
            if cache_policy is not None and spec.cache_policy is None:
                spec.cache_policy = cache_policy
            if error_handler is not None and spec.error_handler_node is None:
                spec.error_handler_node = error_handler
        return self

    def set_entry_point(self, key: str) -> StateGraph:
        return self.add_edge(START, key)

    def set_conditional_entry_point(
        self,
        path: Callable,
        path_map: dict | list | None = None,
    ) -> StateGraph:
        return self.add_conditional_edges(START, path, path_map)

    def set_finish_point(self, key: str) -> StateGraph:
        return self.add_edge(key, END)

    def validate(self, interrupt: list[str] | None = None) -> StateGraph:
        """Validate graph integrity."""
        all_sources = {src for src, _ in self._all_edges}
        for start, branches in self.branches.items():
            all_sources.add(start)
        for name, spec in self.nodes.items():
            if spec.ends:
                all_sources.add(name)

        for source in all_sources:
            if source not in self.nodes and source != START:
                raise ValueError(f"Found edge starting at unknown node '{source}'")

        if START not in all_sources:
            raise ValueError(
                "Graph must have an entrypoint: add at least one edge from START"
            )

        all_targets = {end for _, end in self._all_edges}
        for start, branches in self.branches.items():
            for cond, branch in branches.items():
                if branch.ends is not None:
                    for end in branch.ends.values():
                        if end not in self.nodes and end != END:
                            raise ValueError(
                                f"At '{start}' node, '{cond}' branch found unknown target '{end}'"
                            )
                        all_targets.add(end)

        for target in all_targets:
            if target not in self.nodes and target != END:
                raise ValueError(f"Found edge ending at unknown node `{target}`")

        if interrupt:
            for node in interrupt:
                if node not in self.nodes:
                    raise ValueError(f"Interrupt node `{node}` not found")

        for name, spec in self.nodes.items():
            if spec.error_handler_node and spec.error_handler_node not in self.nodes:
                raise ValueError(
                    f"error_handler node `{spec.error_handler_node}` for `{name}` not found"
                )

        self.compiled = True
        return self

    def compile(
        self,
        checkpointer: BaseCheckpointSaver | None = None,
        *,
        interrupt_before: list[str] | None = None,
        interrupt_after: list[str] | None = None,
        context: dict | None = None,
        cache: Any | None = None,
        store: Any | None = None,
        debug: bool = False,
    ) -> CompiledStateGraph:
        """Compile the StateGraph into an executable CompiledStateGraph.

        Args:
            checkpointer: Checkpoint saver for persistence.
            interrupt_before: Node names to interrupt before execution.
            interrupt_after: Node names to interrupt after execution.
            context: Immutable context values injected into nodes.
            cache: Cache backend for node-level result caching.
            store: Key-value store accessible from nodes.
            debug: Enable debug stream mode.

        Returns:
            A compiled graph ready for ``invoke`` / ``stream``.
        """
        interrupt_before = interrupt_before or []
        interrupt_after = interrupt_after or []

        self.validate(interrupt=interrupt_before + interrupt_after)

        # Auto-add __error__ channel if any node has an error_handler
        has_error_handler = any(spec.error_handler_node for spec in self.nodes.values())
        if has_error_handler and "__error__" not in self.channels:
            from zerograph.channels.last_value import LastValue
            self.channels["__error__"] = LastValue(dict, "__error__")
            self.channels["__error__"].key = "__error__"

        output_channels = (
            "__root__"
            if len(self.schemas[self.output_schema]) == 1
            and "__root__" in self.schemas[self.output_schema]
            else list(self.schemas[self.output_schema].keys())
        )
        stream_channels = (
            "__root__"
            if len(self.channels) == 1 and "__root__" in self.channels
            else list(self.channels.keys())
        )

        compiled = CompiledStateGraph(
            builder=self,
            channels={k: v.from_checkpoint(MISSING) for k, v in self.channels.items()},
            output_channels=output_channels,
            stream_channels=stream_channels,
            checkpointer=checkpointer,
            interrupt_before_nodes=interrupt_before,
            interrupt_after_nodes=interrupt_after,
            context=context,
            cache=cache,
            store=store,
        )
        return compiled

    def get_graph(self) -> str:
        """Generate a Mermaid flowchart diagram of this graph."""
        from zerograph.visualization import get_mermaid
        return get_mermaid(self)


class CompiledStateGraph:
    """Compiled graph that can be executed via ``invoke``, ``stream``, etc.

    Created by calling ``StateGraph.compile()``.  Do not instantiate directly.
    """

    def __init__(
        self,
        *,
        builder: StateGraph,
        channels: dict[str, BaseChannel],
        output_channels: list[str] | str,
        stream_channels: list[str] | str,
        checkpointer: BaseCheckpointSaver | None = None,
        interrupt_before_nodes: list[str] | None = None,
        interrupt_after_nodes: list[str] | None = None,
        context: dict | None = None,
        cache: Any | None = None,
        store: Any | None = None,
    ):
        self.builder = builder
        self._channels = channels
        self._output_channels = output_channels
        self._stream_channels = stream_channels
        self._checkpointer = checkpointer
        self._interrupt_before = interrupt_before_nodes or []
        self._interrupt_after = interrupt_after_nodes or []
        self._context = context
        self._cache = cache
        self._store = store
        self._loop = PregelLoop(
            builder=builder,
            channels=channels,
            output_channels=output_channels,
            stream_channels=stream_channels,
            checkpointer=checkpointer,
            interrupt_before_nodes=interrupt_before_nodes,
            interrupt_after_nodes=interrupt_after_nodes,
            context=context,
            cache=cache,
            store=store,
        )

    def invoke(self, input: Any, config: dict | None = None) -> Any:
        """Execute the graph synchronously.

        Args:
            input: Initial state or state update.
            config: Optional config dict with ``configurable.thread_id`` etc.

        Returns:
            Final state after graph execution completes.
        """
        return self._loop.invoke(input, config)

    def stream(self, input: Any, config: dict | None = None, *,
               stream_mode: str | list[str] = "updates") -> Any:
        """Execute the graph and yield stream events.

        Args:
            input: Initial state or state update.
            config: Optional config dict.
            stream_mode: ``"updates"``, ``"values"``, ``"custom"``,
                ``"messages"``, ``"checkpoints"``, ``"tasks"``, ``"debug"``,
                or a list for multi-mode streaming.

        Returns:
            Generator yielding stream events.
        """
        return self._loop.stream(input, config, stream_mode=stream_mode)

    async def ainvoke(self, input: Any, config: dict | None = None) -> Any:
        """Execute the graph asynchronously.

        Args:
            input: Initial state or state update.
            config: Optional config dict.

        Returns:
            Final state after graph execution completes.
        """
        return await self._loop.ainvoke(input, config)

    async def astream(self, input: Any, config: dict | None = None, *,
                      stream_mode: str | list[str] = "updates"):
        """Execute the graph asynchronously and yield stream events."""
        async for event in self._loop.astream(input, config, stream_mode=stream_mode):
            yield event

    def get_state(self, config: dict | None = None, *, subgraphs: bool = False) -> StateSnapshot:
        """Get a snapshot of the current graph state.

        Args:
            config: Config dict with ``configurable.thread_id``.
            subgraphs: If True, include subgraph states.

        Returns:
            A StateSnapshot containing values, next nodes, metadata, etc.
        """
        return self._loop.get_state(config or {}, subgraphs=subgraphs)

    def get_state_history(self, config: dict | None = None, *, limit: int = 25) -> list:
        """Get all checkpoint snapshots for this thread, newest first.

        Args:
            config: Config dict with ``configurable.thread_id``.
            limit: Maximum number of snapshots to return.

        Returns:
            List of StateSnapshot objects.
        """
        return self._loop.get_state_history(config or {}, limit=limit)

    def update_state(self, config: dict | None, values: Any,
                     as_node: str | None = None) -> dict:
        """Manually update the graph state.

        Args:
            config: Config dict with ``configurable.thread_id``.
            values: State values to update.
            as_node: Pretend the update came from this node (affects ``next``).

        Returns:
            Updated config dict.
        """
        return self._loop.update_state(config or {}, values, as_node)

    def get_graph(self):
        """Return self for compatibility."""
        return self

    def batch(self, inputs: list[Any], config: dict | None = None) -> list[Any]:
        """Execute the graph for multiple inputs sequentially."""
        return [self.invoke(inp, config) for inp in inputs]

    async def abatch(self, inputs: list[Any], config: dict | None = None) -> list[Any]:
        """Execute the graph for multiple inputs concurrently."""
        return await asyncio.gather(
            *[self.ainvoke(inp, config) for inp in inputs]
        )



def _get_channels(schema: type) -> dict[str, BaseChannel]:
    """Extract channels from a TypedDict or similar schema."""
    if not hasattr(schema, "__annotations__"):
        return {"__root__": LastValue(schema, "__root__")}

    type_hints = get_type_hints(schema, include_extras=True)
    channels = {}
    for name, typ in type_hints.items():
        if name == "__slots__":
            continue
        channels[name] = _get_channel(name, typ)

    return channels


def _get_channel(name: str, annotation: Any) -> BaseChannel:
    """Get a channel for a given type annotation."""
    # Handle Annotated types
    if hasattr(annotation, "__metadata__"):
        meta = annotation.__metadata__
        origin = getattr(annotation, "__origin__", annotation)

        for item in meta:
            if isinstance(item, BaseChannel):
                ch = item.__class__(origin if hasattr(annotation, "__origin__") else annotation)
                ch.key = name
                return ch
            elif isinstance(item, type) and issubclass(item, BaseChannel):
                ch = item(origin if hasattr(annotation, "__origin__") else annotation)
                ch.key = name
                return ch

        # Check if last metadata item is a reducer (binary operator)
        if meta and callable(meta[-1]):
            sig = signature(meta[-1])
            params = list(sig.parameters.values())
            positional = sum(
                p.kind in (p.POSITIONAL_ONLY, p.POSITIONAL_OR_KEYWORD)
                for p in params
            )
            if positional == 2:
                ch = BinaryOperatorAggregate(origin if hasattr(annotation, "__origin__") else annotation, meta[-1])
                ch.key = name
                return ch

    ch = LastValue(annotation, name)
    ch.key = name
    return ch
