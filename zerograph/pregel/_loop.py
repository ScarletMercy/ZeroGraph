"""Pregel execution loop - simplified graph execution engine."""

from __future__ import annotations

import asyncio
import copy
import inspect
import itertools
import threading
import time
import uuid
from collections import defaultdict
from collections.abc import Generator, AsyncGenerator, Callable
from contextvars import ContextVar
from datetime import datetime, timezone
from typing import Any

from zerograph._internal import EMPTY_SEQ, MISSING
from zerograph.channels.base import BaseChannel
from zerograph.channels.binop import BinaryOperatorAggregate
from zerograph.checkpoint.base import Checkpoint, CheckpointMetadata
from zerograph.constants import (
    CONFIG_KEY_CACHE,
    CONFIG_KEY_CHECKPOINT_NS,
    CONFIG_KEY_CONTEXT,
    CONFIG_KEY_READ,
    CONFIG_KEY_SEND,
    CONFIG_KEY_STORE,
    CONFIG_KEY_TASK_ID,
    CONFIG_KEY_WRITER,
    END,
    INTERRUPT,
    NULL_TASK_ID,
    RESUME,
    START,
)
from zerograph.errors import (
    EmptyChannelError,
    GraphBubbleUp,
    GraphInterrupt,
    GraphRecursionError,
    ParentCommand,
)
from zerograph.types import (
    Command,
    Interrupt,
    Overwrite,
    PregelTask,
    RetryPolicy,
    Send,
    StateSnapshot,
)

_current_config: ContextVar[dict] = ContextVar("_current_config", default={})

__all__ = ("PregelLoop",)


class _LazyAtomicCounter:
    _lock = threading.Lock()

    def __init__(self):
        self._counter = None

    def __call__(self) -> int:
        if self._counter is None:
            with self._lock:
                if self._counter is None:
                    self._counter = itertools.count(0).__next__
        return self._counter()


class Scratchpad:
    __slots__ = ("step", "stop", "resume", "get_null_resume",
                 "interrupt_counter", "call_counter")

    def __init__(self, *, step=0, stop=25, resume=None, get_null_resume=None):
        self.step = step
        self.stop = stop
        self.resume = resume or []
        self.get_null_resume = get_null_resume or (lambda consume=False: None)
        self.interrupt_counter = _LazyAtomicCounter()
        self.call_counter = _LazyAtomicCounter()


class PregelLoop:
    """Graph execution engine using a simplified Pregel-like superstep model."""

    def __init__(
        self,
        *,
        builder,
        channels: dict[str, BaseChannel],
        output_channels: list[str] | str,
        stream_channels: list[str] | str,
        checkpointer=None,
        interrupt_before_nodes: list[str] | None = None,
        interrupt_after_nodes: list[str] | None = None,
        context: dict | None = None,
        cache: Any | None = None,
        store: Any | None = None,
    ):
        self.builder = builder
        self.channels = channels
        self.output_channels = output_channels
        self.stream_channels = stream_channels
        self.checkpointer = checkpointer
        self.interrupt_before = interrupt_before_nodes or []
        self.interrupt_after = interrupt_after_nodes or []
        self.context = context
        self.cache = cache
        self.store = store

    def invoke(self, input: Any, config: dict | None = None) -> Any:
        config = config or {}
        result = None
        for event in self._run(input, config, stream_mode="values"):
            result = event
        return result

    def stream(self, input: Any, config: dict | None = None, *,
               stream_mode: str | list[str] = "updates"):
        config = config or {}
        return self._run(input, config, stream_mode=stream_mode)

    async def ainvoke(self, input: Any, config: dict | None = None) -> Any:
        config = config or {}
        result = None
        async for event in self._arun(input, config, stream_mode="values"):
            result = event
        return result

    async def astream(self, input: Any, config: dict | None = None, *,
                      stream_mode: str | list[str] = "updates"):
        """Execute the graph asynchronously and yield stream events."""
        config = config or {}
        async for event in self._arun(input, config, stream_mode=stream_mode):
            yield event

    def get_state(self, config: dict, *, subgraphs: bool = False) -> StateSnapshot:
        checkpoint = self._load_checkpoint(config)
        if checkpoint is None:
            return StateSnapshot(
                values=self._read_output(self.channels),
                next=(),
                config=config,
                metadata=None,
                created_at=None,
                parent_config=None,
                tasks=(),
                interrupts=(),
            )

        channels = self._restore_channels(checkpoint)

        pending_writes = self._get_pending_writes(config)
        interrupts = []
        for tid, ch, val in pending_writes:
            if ch == INTERRUPT and isinstance(val, Interrupt):
                interrupts.append(val)

        subgraph_states = None
        if subgraphs and self.checkpointer is not None:
            subgraph_states = self._get_subgraph_states(config, checkpoint)

        return StateSnapshot(
            values=self._read_output(channels),
            next=tuple(checkpoint.get("_next_nodes", ())),
            config=config,
            metadata=checkpoint.get("_metadata"),
            created_at=checkpoint.get("ts"),
            parent_config=None,
            tasks=(),
            interrupts=tuple(interrupts),
            subgraphs=subgraph_states,
        )

    def _get_subgraph_states(self, config: dict, checkpoint: dict) -> dict[str, StateSnapshot]:
        """Recursively get states of all subgraphs."""
        from zerograph.constants import NS_SEP
        parent_ns = config.get("configurable", {}).get(CONFIG_KEY_CHECKPOINT_NS, "")
        subgraph_states = {}
        for node_name, spec in self.builder.nodes.items():
            if hasattr(spec.runnable, '_loop') and hasattr(spec.runnable, 'builder'):
                sub_ns = f"{parent_ns}{NS_SEP}{node_name}" if parent_ns else node_name
                # Build sub_config without parent's checkpoint_id — subgraph has its own
                parent_configurable = {k: v for k, v in config.get("configurable", {}).items()
                                       if k != "checkpoint_id"}
                sub_config = {
                    "configurable": {
                        **parent_configurable,
                        CONFIG_KEY_CHECKPOINT_NS: sub_ns,
                    }
                }
                sub_cp = self._load_checkpoint(sub_config)
                if sub_cp is not None:
                    sub_channels = self._restore_channels(sub_cp)
                    # Recursively get nested subgraph states using the subgraph's builder
                    # but with the parent's checkpointer (subgraphs don't own checkpointers)
                    sub_loop = spec.runnable._loop
                    nested = self._get_nested_subgraph_states(
                        sub_loop, sub_config, sub_cp
                    )
                    pending = self._get_pending_writes(sub_config)
                    sub_interrupts = [val for tid, ch, val in pending
                                      if ch == INTERRUPT and isinstance(val, Interrupt)]
                    subgraph_states[node_name] = StateSnapshot(
                        values=self._read_output(sub_channels),
                        next=tuple(sub_cp.get("_next_nodes", ())),
                        config=sub_config,
                        metadata=sub_cp.get("_metadata"),
                        created_at=sub_cp.get("ts"),
                        parent_config=None,
                        tasks=(),
                        interrupts=tuple(sub_interrupts),
                        subgraphs=nested if nested else None,
                    )
        return subgraph_states

    def _get_nested_subgraph_states(
        self, sub_loop: PregelLoop, config: dict, checkpoint: dict
    ) -> dict[str, StateSnapshot]:
        """Get nested subgraph states using sub_loop's builder but this loop's checkpointer."""
        from zerograph.constants import NS_SEP
        parent_ns = config.get("configurable", {}).get(CONFIG_KEY_CHECKPOINT_NS, "")
        subgraph_states = {}
        for node_name, spec in sub_loop.builder.nodes.items():
            if hasattr(spec.runnable, '_loop') and hasattr(spec.runnable, 'builder'):
                sub_ns = f"{parent_ns}{NS_SEP}{node_name}" if parent_ns else node_name
                parent_configurable = {k: v for k, v in config.get("configurable", {}).items()
                                       if k != "checkpoint_id"}
                sub_config = {
                    "configurable": {
                        **parent_configurable,
                        CONFIG_KEY_CHECKPOINT_NS: sub_ns,
                    }
                }
                # Use parent's checkpointer to load the nested subgraph's checkpoint
                sub_cp = self._load_checkpoint(sub_config)
                if sub_cp is not None:
                    sub_channels = self._restore_channels(sub_cp)
                    inner_loop = spec.runnable._loop
                    nested = self._get_nested_subgraph_states(inner_loop, sub_config, sub_cp)
                    pending = self._get_pending_writes(sub_config)
                    sub_interrupts = [val for tid, ch, val in pending
                                      if ch == INTERRUPT and isinstance(val, Interrupt)]
                    subgraph_states[node_name] = StateSnapshot(
                        values=self._read_output(sub_channels),
                        next=tuple(sub_cp.get("_next_nodes", ())),
                        config=sub_config,
                        metadata=sub_cp.get("_metadata"),
                        created_at=sub_cp.get("ts"),
                        parent_config=None,
                        tasks=(),
                        interrupts=tuple(sub_interrupts),
                        subgraphs=nested if nested else None,
                    )
        return subgraph_states

    def get_state_history(self, config: dict, *, limit: int = 25) -> list[StateSnapshot]:
        """List all checkpoint snapshots for this thread, newest first."""
        if self.checkpointer is None:
            return []
        tuples = self.checkpointer.list(config, limit=limit)
        snapshots = []
        for tup in tuples:
            channels = self._restore_channels(tup.checkpoint)
            pending_writes = tup.pending_writes or []
            interrupts = []
            for tid, ch, val in pending_writes:
                if ch == INTERRUPT and isinstance(val, Interrupt):
                    interrupts.append(val)
            snapshots.append(
                StateSnapshot(
                    values=self._read_output(channels),
                    next=tuple(tup.checkpoint.get("_next_nodes", ())),
                    config=tup.config,
                    metadata=tup.metadata,
                    created_at=tup.checkpoint.get("ts"),
                    parent_config=tup.parent_config,
                    tasks=(),
                    interrupts=tuple(interrupts),
                )
            )
        return snapshots

    def update_state(self, config: dict, values: Any, as_node: str | None = None) -> dict:
        checkpoint = self._load_checkpoint(config)
        if checkpoint is None:
            checkpoint = self._create_empty_checkpoint()

        channels = self._restore_channels(checkpoint)

        if isinstance(values, dict):
            for key, val in values.items():
                if key in channels:
                    channels[key].update([val])
        elif "__root__" in channels:
            channels["__root__"].update([values])

        new_cp = self._checkpoint_from_channels(channels, checkpoint)
        new_cp["_metadata"] = CheckpointMetadata(
            source="update", step=new_cp.get("_step", -1) + 1
        )

        # Determine next nodes based on as_node
        if as_node is not None:
            next_nodes = set()
            # Direct edges from as_node
            for start, end in self.builder.edges:
                if start == as_node and end != END:
                    next_nodes.add(end)
            # Conditional edges from as_node — evaluate the path function
            # with the updated state to get the correct routing target
            if as_node in self.builder.branches:
                state_for_route = self._read_output(channels)
                for name, branch in self.builder.branches[as_node].items():
                    try:
                        path_result = branch.path(state_for_route)
                    except Exception:
                        # If path evaluation fails, include all possible targets
                        if branch.ends:
                            next_nodes.update(branch.ends.values())
                        continue
                    # Resolve the path result to node name(s)
                    if isinstance(path_result, list):
                        for item in path_result:
                            if hasattr(item, 'node'):
                                target = item.node
                            else:
                                target = str(item)
                            if target != END:
                                next_nodes.add(target)
                    else:
                        target = str(path_result)
                        if target != END:
                            next_nodes.add(target)
            # Remove as_node itself and START
            next_nodes.discard(as_node)
            next_nodes.discard(START)
            new_cp["_next_nodes"] = list(next_nodes)

        return self._save_checkpoint(config, new_cp)

    # ---- Sync execution ----

    @staticmethod
    def _norm_modes(stream_mode: str | list[str]) -> set[str]:
        if isinstance(stream_mode, str):
            return {stream_mode}
        return set(stream_mode)

    def _run(self, input: Any, config: dict, *, stream_mode: str | list[str]) -> Generator:
        modes = self._norm_modes(stream_mode)
        is_multi = isinstance(stream_mode, list)
        recursion_limit = config.get("recursion_limit", 25)
        configurable = config.setdefault("configurable", {})

        # Load checkpoint or create fresh
        checkpoint = self._load_checkpoint(config)
        is_resuming = checkpoint is not None and isinstance(input, Command)

        if checkpoint is None:
            checkpoint = self._create_empty_checkpoint()

        channels = self._restore_channels(checkpoint)
        pending_writes = self._get_pending_writes(config) if is_resuming else []

        # Extract resume value from pending_writes or input
        resume_value = None
        for tid, ch, val in pending_writes:
            if ch == RESUME:
                resume_value = val

        # Apply input
        if isinstance(input, Command):
            if input.update:
                self._apply_input(input.update, channels)
            if input.resume is not None:
                resume_value = input.resume
        elif isinstance(input, dict):
            self._apply_input(input, channels)
        else:
            self._apply_input({"__root__": input}, channels)

        # Apply pending writes from checkpoint (non-RESUME channel writes)
        for tid, ch, val in pending_writes:
            if ch == RESUME:
                continue
            if ch in channels and val is not None:
                channels[ch].update([val])

        # Apply context (immutable runtime values)
        self._apply_context(channels)

        # Determine starting nodes from edges
        builder = self.builder
        next_nodes, initial_sends = self._get_start_nodes(checkpoint, is_resuming, channels)

        # Track Send inputs: node_name -> list of args (run node once per Send)
        send_inputs: dict[str, list[Any]] = defaultdict(list)

        # Process initial Send objects
        if initial_sends:
            for send in initial_sends:
                if send.node in builder.nodes:
                    send_inputs[send.node].append(send.arg)
            for send in initial_sends:
                if send.node in builder.nodes and send.node not in next_nodes:
                    next_nodes.append(send.node)

        # Save initial checkpoint
        checkpoint = self._checkpoint_from_channels(channels, checkpoint)
        checkpoint["_next_nodes"] = next_nodes
        checkpoint["_step"] = 0
        self._save_checkpoint(config, checkpoint)

        last_event = None
        completed_nodes: set[str] = set()

        for step in range(recursion_limit + 1):
            if step >= recursion_limit:
                raise GraphRecursionError(
                    f"Recursion limit of {recursion_limit} reached"
                )

            if not next_nodes:
                break

            # Check waiting edges: add nodes whose all sources have completed
            for starts, end in builder.waiting_edges:
                if (end not in next_nodes
                    and end not in completed_nodes
                    and all(s in completed_nodes for s in starts)):
                    if end != END:
                        next_nodes.append(end)

            # Check interrupt_before (only skip on the first step of a resume)
            skip_interrupt = is_resuming and step == 0
            if self.interrupt_before and not skip_interrupt:
                nodes_to_interrupt = [
                    n for n in next_nodes
                    if ("*" in self.interrupt_before or n in self.interrupt_before)
                    and n in builder.nodes
                ]
                if nodes_to_interrupt:
                    checkpoint = self._checkpoint_from_channels(channels, checkpoint)
                    checkpoint["_next_nodes"] = next_nodes
                    self._save_checkpoint(config, checkpoint)
                    if "values" in modes:
                        val = self._read_output(channels)
                        yield ("values", val) if is_multi else val
                    return

            # Execute nodes
            updates = {}
            new_next = set()
            new_sends: list[Send] = []

            for node_name in next_nodes:
                if node_name not in builder.nodes:
                    continue

                node_spec = builder.nodes[node_name]
                node_func = node_spec.runnable

                if "debug" in modes:
                    dbg_ev = {"type": "task", "step": step, "node": node_name, "state": self._read_output(channels)}
                    yield ("debug", dbg_ev) if is_multi else dbg_ev

                if "tasks" in modes:
                    yield ("tasks", {"type": "task_start", "step": step, "node": node_name}) if is_multi else {"type": "task_start", "step": step, "node": node_name}

                # Handle Send-triggered nodes (run once per Send arg)
                if node_name in send_inputs:
                    send_custom_events: list = []
                    any_success = False
                    last_result = None
                    send_errors: list[Exception] = []
                    for send_arg in send_inputs[node_name]:
                        # Create fresh task config per Send arg for unique task_id
                        send_scratchpad = Scratchpad(
                            step=step, stop=recursion_limit,
                            resume=[resume_value] if resume_value is not None else None
                        )
                        send_custom_writer = None
                        if "custom" in modes:
                            def _make_si_writer(n, evts):
                                def writer(value):
                                    evts.append((n, value))
                                return writer
                            send_custom_writer = _make_si_writer(node_name, send_custom_events)
                        send_task_config = self._make_task_config(
                            config, node_name, channels, send_scratchpad,
                            custom_writer=send_custom_writer,
                        )
                        send_token = _current_config.set(send_task_config)
                        try:
                            result = self._call_node(
                                node_func, send_arg,
                                retry_policy=node_spec.retry_policy,
                                timeout=node_spec.timeout.run_timeout if node_spec.timeout else None,
                            )
                        except GraphBubbleUp:
                            _current_config.reset(send_token)
                            raise
                        except Exception as send_err:
                            _current_config.reset(send_token)
                            send_errors.append(send_err)
                            continue
                        _current_config.reset(send_token)

                        any_success = True
                        last_result = result
                        if result is not None:
                            result_updates = self._process_result(result, channels)
                            if result_updates and "updates" in modes:
                                updates.setdefault(node_name, {}).update(result_updates)
                            if isinstance(result, Send):
                                new_sends.append(result)
                            elif isinstance(result, (list, tuple)):
                                for item in result:
                                    if isinstance(item, Send):
                                        new_sends.append(item)

                    # Yield custom writer events
                    if "custom" in modes and send_custom_events:
                        for cn, cv in send_custom_events:
                            yield ("custom", {"node": cn, "value": cv}) if is_multi else {"node": cn, "value": cv}
                    if any_success:
                        completed_nodes.add(node_name)
                        node_next, extra_sends = self._get_next_nodes(builder, node_name, last_result, channels)
                        new_next.update(node_next)
                        new_sends.extend(extra_sends)
                    elif send_errors:
                        raise send_errors[-1]
                    continue

                # Regular node execution
                try:
                    node_input = self._read_node_input(channels, node_spec, builder)
                except EmptyChannelError:
                    # Channel not yet populated — skip this node for now
                    continue
                except Exception:
                    if node_spec.error_handler_node:
                        error_val = {"node": node_name, "error": "input read failed"}
                        if "__error__" in channels:
                            channels["__error__"].update([error_val])
                        new_next.add(node_spec.error_handler_node)
                        completed_nodes.add(node_name)
                        continue
                    raise

                # Check cache
                cache_key = self._get_cache_key(
                    node_name, node_input, node_spec.cache_policy
                )
                cached_result = self._check_cache(cache_key, node_spec.cache_policy)
                if cached_result is not None:
                    result = cached_result
                    completed_nodes.add(node_name)
                    if result is not None:
                        result_updates = self._process_result(result, channels)
                        if result_updates and "updates" in modes:
                            updates[node_name] = result_updates
                    node_next, extra_sends = self._get_next_nodes(builder, node_name, result, channels)
                    new_next.update(node_next)
                    new_sends.extend(extra_sends)
                    continue

                # Create scratchpad with resume value for interrupted nodes
                scratchpad = Scratchpad(step=step, stop=recursion_limit,
                                        resume=[resume_value] if resume_value is not None else None)

                # Setup custom writer for "custom" stream mode
                custom_events: list = []
                custom_writer = None
                if "custom" in modes:
                    def _make_writer(n):
                        def writer(value):
                            custom_events.append((n, value))
                        return writer
                    custom_writer = _make_writer(node_name)

                task_config = self._make_task_config(
                    config, node_name, channels, scratchpad,
                    custom_writer=custom_writer,
                )

                # Handle generator nodes for "messages" stream mode
                is_gen = inspect.isgeneratorfunction(node_func)

                token = _current_config.set(task_config)
                try:
                    if is_gen and "messages" in modes:
                        # Run generator and stream chunks
                        needs_conf = self._func_needs_config(node_func)
                        args = (node_input, task_config) if needs_conf else (node_input,)
                        gen = node_func(*args)
                        result = None
                        try:
                            while True:
                                chunk = next(gen)
                                yield ("messages", {"node": node_name, "chunk": chunk}) if is_multi else {"node": node_name, "chunk": chunk}
                        except StopIteration as e:
                            result = e.value
                    else:
                        result = self._call_node(
                            node_func, node_input,
                            retry_policy=node_spec.retry_policy,
                            timeout=node_spec.timeout.run_timeout if node_spec.timeout else None,
                        )
                except GraphInterrupt as gi:
                    _current_config.reset(token)
                    checkpoint = self._checkpoint_from_channels(channels, checkpoint)
                    checkpoint["_next_nodes"] = (node_name,)
                    for interrupt_val in gi.interrupts:
                        self._save_interrupt(config, checkpoint, interrupt_val)
                    if "values" in modes:
                        val = self._read_output(channels)
                        yield ("values", val) if is_multi else val
                    return
                except GraphBubbleUp:
                    _current_config.reset(token)
                    raise
                except Exception as e:
                    _current_config.reset(token)
                    if node_spec.error_handler_node:
                        error_val = {"node": node_name, "error": str(e)}
                        if "__error__" in channels:
                            channels["__error__"].update([error_val])
                        new_next.add(node_spec.error_handler_node)
                        completed_nodes.add(node_name)
                        if "tasks" in modes:
                            yield ("tasks", {"type": "task_end", "step": step, "node": node_name}) if is_multi else {"type": "task_end", "step": step, "node": node_name}
                        continue
                    raise

                _current_config.reset(token)
                completed_nodes.add(node_name)

                if "tasks" in modes:
                    yield ("tasks", {"type": "task_end", "step": step, "node": node_name}) if is_multi else {"type": "task_end", "step": step, "node": node_name}

                # Store result in cache
                if cache_key is not None and result is not None:
                    self._store_cache(cache_key, node_spec.cache_policy, result)

                # Yield custom writer events
                if "custom" in modes and custom_events:
                    for cn, cv in custom_events:
                        yield ("custom", {"node": cn, "value": cv}) if is_multi else {"node": cn, "value": cv}

                # Process result
                if result is not None:
                    result_updates = self._process_result(result, channels)
                    if result_updates and "updates" in modes:
                        updates[node_name] = result_updates

                    # Only add direct Send results if there are no conditional
                    # edges on this node — conditional edges will produce
                    # their own sends via _get_next_nodes below.
                    if node_name not in builder.branches:
                        if isinstance(result, Send):
                            new_sends.append(result)
                        elif isinstance(result, (list, tuple)):
                            for item in result:
                                if isinstance(item, Send):
                                    new_sends.append(item)

                if "debug" in modes:
                    dbg_ev = {"type": "task_result", "step": step, "node": node_name, "result": result}
                    yield ("debug", dbg_ev) if is_multi else dbg_ev

                # Determine next nodes from edges
                node_next, extra_sends = self._get_next_nodes(builder, node_name, result, channels)
                new_next.update(node_next)
                new_sends.extend(extra_sends)

            # Process new sends from this step (with nested Send support)
            i = 0
            while i < len(new_sends):
                send = new_sends[i]
                i += 1
                if send.node in builder.nodes:
                    node_spec = builder.nodes[send.node]
                    # Build proper task config for the Send-target node
                    send_scratchpad = Scratchpad(
                        step=step, stop=recursion_limit,
                        resume=[resume_value] if resume_value is not None else None
                    )
                    send_custom_events: list = []
                    send_custom_writer = None
                    if "custom" in modes:
                        def _make_send_writer(n):
                            def writer(value):
                                send_custom_events.append((n, value))
                            return writer
                        send_custom_writer = _make_send_writer(send.node)
                    send_config = self._make_task_config(
                        config, send.node, channels, send_scratchpad,
                        custom_writer=send_custom_writer,
                    )
                    send_token = _current_config.set(send_config)
                    try:
                        # Check cache for send-target
                        cache_key = self._get_cache_key(
                            send.node, send.arg, node_spec.cache_policy
                        )
                        cached = self._check_cache(cache_key, node_spec.cache_policy)
                        if cached is not None:
                            result = cached
                        else:
                            result = self._call_node(
                                node_spec.runnable, send.arg,
                                retry_policy=node_spec.retry_policy,
                                timeout=node_spec.timeout.run_timeout if node_spec.timeout else None,
                            )
                            if cache_key is not None and result is not None:
                                self._store_cache(cache_key, node_spec.cache_policy, result)
                    except GraphBubbleUp:
                        _current_config.reset(send_token)
                        raise
                    except Exception as send_err:
                        _current_config.reset(send_token)
                        # Propagate Send errors instead of silently swallowing
                        if node_spec.error_handler_node:
                            error_val = {"node": send.node, "error": str(send_err)}
                            if "__error__" in channels:
                                channels["__error__"].update([error_val])
                            new_next.add(node_spec.error_handler_node)
                            completed_nodes.add(send.node)
                            continue
                        raise
                    _current_config.reset(send_token)

                    # Yield custom writer events
                    if "custom" in modes and send_custom_events:
                        for cn, cv in send_custom_events:
                            yield ("custom", {"node": cn, "value": cv}) if is_multi else {"node": cn, "value": cv}

                    if result is not None:
                        result_updates = self._process_result(result, channels)
                        if result_updates and "updates" in modes:
                            updates.setdefault(send.node, {}).update(result_updates)
                        if isinstance(result, Send):
                            new_sends.append(result)
                        elif isinstance(result, (list, tuple)):
                            for item in result:
                                if isinstance(item, Send):
                                    new_sends.append(item)
                    completed_nodes.add(send.node)
                    node_next, extra_sends = self._get_next_nodes(builder, send.node, result, channels)
                    new_next.update(node_next)
                    new_sends.extend(extra_sends)

            # Check interrupt_after
            if self.interrupt_after:
                nodes_to_interrupt = [
                    n for n in next_nodes
                    if ("*" in self.interrupt_after or n in self.interrupt_after)
                    and n in builder.nodes
                ]
                if nodes_to_interrupt:
                    checkpoint = self._checkpoint_from_channels(channels, checkpoint)
                    checkpoint["_next_nodes"] = list(new_next)
                    self._save_checkpoint(config, checkpoint)
                    if "values" in modes:
                        val = self._read_output(channels)
                        yield ("values", val) if is_multi else val
                    if "updates" in modes:
                        if updates:
                            yield ("updates", updates) if is_multi else updates
                    return

            # Save checkpoint
            checkpoint = self._checkpoint_from_channels(channels, checkpoint)
            checkpoint["_next_nodes"] = list(new_next)
            checkpoint["_step"] = step + 1
            self._save_checkpoint(config, checkpoint)

            # Yield events
            if "checkpoints" in modes:
                cp_event = {"step": step, "next_nodes": list(new_next), "checkpoint_id": checkpoint.get("id")}
                yield ("checkpoints", cp_event) if is_multi else cp_event
            if "values" in modes:
                last_event = self._read_output(channels)
                yield ("values", last_event) if is_multi else last_event
            if "updates" in modes:
                if updates:
                    yield ("updates", updates) if is_multi else updates
            if "debug" in modes:
                dbg_ev = {"type": "step_end", "step": step, "next_nodes": list(new_next)}
                yield ("debug", dbg_ev) if is_multi else dbg_ev

            # After executing all nodes, check waiting edges for next step
            waiting_next = set()
            for starts, end in builder.waiting_edges:
                if (end not in new_next
                    and end not in completed_nodes
                    and all(s in completed_nodes for s in starts)):
                    if end != END:
                        waiting_next.add(end)

            next_nodes = list(new_next | waiting_next)

        if "values" in modes and last_event is None:
            val = self._read_output(channels)
            yield ("values", val) if is_multi else val

    # ---- Async execution ----

    async def _arun(self, input: Any, config: dict, *, stream_mode: str | list[str]) -> AsyncGenerator:
        modes = self._norm_modes(stream_mode)
        is_multi = isinstance(stream_mode, list)
        recursion_limit = config.get("recursion_limit", 25)
        max_concurrency = max(config.get("max_concurrency", 1), 1)
        configurable = config.setdefault("configurable", {})

        checkpoint = self._load_checkpoint(config)
        is_resuming = checkpoint is not None and isinstance(input, Command)

        if checkpoint is None:
            checkpoint = self._create_empty_checkpoint()

        channels = self._restore_channels(checkpoint)
        pending_writes = self._get_pending_writes(config) if is_resuming else []

        # Extract resume value from pending_writes or input
        resume_value = None
        for tid, ch, val in pending_writes:
            if ch == RESUME:
                resume_value = val

        if isinstance(input, Command):
            if input.update:
                self._apply_input(input.update, channels)
            if input.resume is not None:
                resume_value = input.resume
        elif isinstance(input, dict):
            self._apply_input(input, channels)
        else:
            self._apply_input({"__root__": input}, channels)

        # Apply pending writes from checkpoint (non-RESUME)
        for tid, ch, val in pending_writes:
            if ch == RESUME:
                continue
            if ch in channels and val is not None:
                channels[ch].update([val])

        # Apply context (immutable runtime values)
        self._apply_context(channels)

        builder = self.builder
        next_nodes, initial_sends = self._get_start_nodes(checkpoint, is_resuming, channels)

        send_inputs: dict[str, list] = defaultdict(list)
        if initial_sends:
            for send in initial_sends:
                if send.node in builder.nodes:
                    send_inputs[send.node].append(send.arg)
            for send in initial_sends:
                if send.node in builder.nodes and send.node not in next_nodes:
                    next_nodes.append(send.node)

        # Save initial checkpoint
        checkpoint = self._checkpoint_from_channels(channels, checkpoint)
        checkpoint["_next_nodes"] = next_nodes
        checkpoint["_step"] = 0
        self._save_checkpoint(config, checkpoint)

        last_event = None
        completed_nodes: set[str] = set()

        for step in range(recursion_limit + 1):
            if step >= recursion_limit:
                raise GraphRecursionError(f"Recursion limit of {recursion_limit} reached")

            if not next_nodes:
                break

            # Check waiting edges
            for starts, end in builder.waiting_edges:
                if (end not in next_nodes
                    and end not in completed_nodes
                    and all(s in completed_nodes for s in starts)):
                    if end != END:
                        next_nodes.append(end)

            # Check interrupt_before (only skip on the first step of a resume)
            skip_interrupt = is_resuming and step == 0
            if self.interrupt_before and not skip_interrupt:
                nodes_to_interrupt = [
                    n for n in next_nodes
                    if ("*" in self.interrupt_before or n in self.interrupt_before)
                    and n in builder.nodes
                ]
                if nodes_to_interrupt:
                    checkpoint = self._checkpoint_from_channels(channels, checkpoint)
                    checkpoint["_next_nodes"] = next_nodes
                    self._save_checkpoint(config, checkpoint)
                    if "values" in modes:
                        val = self._read_output(channels)
                        yield ("values", val) if is_multi else val
                    return

            updates = {}
            new_next = set()
            new_sends = []

            # Use parallel execution when max_concurrency > 1 and no streaming modes
            # that require sequential yield (messages, custom).
            # Exclude send_inputs nodes — they need per-Send-arg iteration.
            can_parallel = (
                max_concurrency > 1
                and len(next_nodes) > 1
                and "messages" not in modes
                and not any(n in send_inputs for n in next_nodes)
            )

            if can_parallel:
                (par_updates, par_new_next, par_new_sends,
                 par_events, par_results) = await self._arun_parallel_step(
                    next_nodes, channels, config, modes, is_multi,
                    step, recursion_limit, resume_value, max_concurrency,
                )
                updates.update(par_updates)
                new_next = par_new_next
                new_sends = par_new_sends
                for ev in par_events:
                    yield ev if is_multi else ev[1]
                # Mark completed nodes
                for nn in par_results:
                    if par_results[nn].get("error") is None or par_results[nn].get("new_next"):
                        completed_nodes.add(nn)
                # Check for interrupts
                for nn in par_results:
                    if par_results[nn].get("error") == "interrupt":
                        checkpoint = self._checkpoint_from_channels(channels, checkpoint)
                        checkpoint["_next_nodes"] = (nn,)
                        self._save_interrupt(config, checkpoint, None)
                        if "values" in modes:
                            yield ("values", self._read_output(channels)) if is_multi else self._read_output(channels)
                        return
                    if isinstance(par_results[nn].get("error"), Exception):
                        raise par_results[nn]["error"]

            for node_name in next_nodes:
                if can_parallel:
                    continue  # Already executed above
                if node_name not in builder.nodes:
                    continue

                node_spec = builder.nodes[node_name]
                node_func = node_spec.runnable

                if "debug" in modes:
                    dbg_ev = {"type": "task", "step": step, "node": node_name, "state": self._read_output(channels)}
                    yield ("debug", dbg_ev) if is_multi else dbg_ev

                if "tasks" in modes:
                    yield ("tasks", {"type": "task_start", "step": step, "node": node_name}) if is_multi else {"type": "task_start", "step": step, "node": node_name}

                if node_name in send_inputs:
                    send_custom_events: list = []
                    any_success = False
                    last_result = None
                    send_errors: list[Exception] = []
                    for send_arg in send_inputs[node_name]:
                        send_scratchpad = Scratchpad(
                            step=step, stop=recursion_limit,
                            resume=[resume_value] if resume_value is not None else None
                        )
                        send_custom_writer = None
                        if "custom" in modes:
                            def _make_si_writer(n, evts):
                                def writer(value):
                                    evts.append((n, value))
                                return writer
                            send_custom_writer = _make_si_writer(node_name, send_custom_events)
                        send_task_config = self._make_task_config(
                            config, node_name, channels, send_scratchpad,
                            custom_writer=send_custom_writer,
                        )
                        send_token = _current_config.set(send_task_config)
                        try:
                            result = await self._acall_node(
                                node_func, send_arg,
                                retry_policy=node_spec.retry_policy,
                                timeout=node_spec.timeout.run_timeout if node_spec.timeout else None,
                            )
                        except GraphBubbleUp:
                            _current_config.reset(send_token)
                            raise
                        except Exception as send_err:
                            _current_config.reset(send_token)
                            send_errors.append(send_err)
                            continue
                        _current_config.reset(send_token)
                        any_success = True
                        last_result = result
                        if result is not None:
                            result_updates = self._process_result(result, channels)
                            if result_updates and "updates" in modes:
                                updates.setdefault(node_name, {}).update(result_updates)
                            if isinstance(result, Send):
                                new_sends.append(result)
                            elif isinstance(result, (list, tuple)):
                                for item in result:
                                    if isinstance(item, Send):
                                        new_sends.append(item)

                    # Yield custom writer events
                    if "custom" in modes and send_custom_events:
                        for cn, cv in send_custom_events:
                            yield ("custom", {"node": cn, "value": cv}) if is_multi else {"node": cn, "value": cv}
                    if any_success:
                        completed_nodes.add(node_name)
                        node_next, extra_sends = self._get_next_nodes(builder, node_name, last_result, channels)
                        new_next.update(node_next)
                        new_sends.extend(extra_sends)
                    elif send_errors:
                        raise send_errors[-1]
                    continue

                try:
                    node_input = self._read_node_input(channels, node_spec, builder)
                except EmptyChannelError:
                    continue
                except Exception:
                    if node_spec.error_handler_node:
                        error_val = {"node": node_name, "error": "input read failed"}
                        if "__error__" in channels:
                            channels["__error__"].update([error_val])
                        new_next.add(node_spec.error_handler_node)
                        completed_nodes.add(node_name)
                        continue
                    raise

                # Check cache
                cache_key = self._get_cache_key(
                    node_name, node_input, node_spec.cache_policy
                )
                cached_result = self._check_cache(cache_key, node_spec.cache_policy)
                if cached_result is not None:
                    result = cached_result
                    completed_nodes.add(node_name)
                    if result is not None:
                        result_updates = self._process_result(result, channels)
                        if result_updates and "updates" in modes:
                            updates[node_name] = result_updates
                    node_next, extra_sends = self._get_next_nodes(builder, node_name, result, channels)
                    new_next.update(node_next)
                    new_sends.extend(extra_sends)
                    continue

                scratchpad = Scratchpad(step=step, stop=recursion_limit,
                                        resume=[resume_value] if resume_value is not None else None)

                # Setup custom writer for "custom" stream mode
                custom_events: list = []
                custom_writer = None
                if "custom" in modes:
                    def _make_writer(n):
                        def writer(value):
                            custom_events.append((n, value))
                        return writer
                    custom_writer = _make_writer(node_name)

                task_config = self._make_task_config(
                    config, node_name, channels, scratchpad,
                    custom_writer=custom_writer,
                )

                # Handle async generator nodes for "messages" stream mode
                is_async_gen = inspect.isasyncgenfunction(node_func)
                is_sync_gen = inspect.isgeneratorfunction(node_func)

                token = _current_config.set(task_config)
                try:
                    if is_async_gen and "messages" in modes:
                        # Run async generator and stream chunks
                        needs_conf = self._func_needs_config(node_func)
                        args = (node_input, task_config) if needs_conf else (node_input,)
                        gen = node_func(*args)
                        result = None
                        try:
                            while True:
                                chunk = await gen.__anext__()
                                yield ("messages", {"node": node_name, "chunk": chunk}) if is_multi else {"node": node_name, "chunk": chunk}
                        except StopAsyncIteration as e:
                            result = e.value
                    elif is_sync_gen and "messages" in modes:
                        # Sync generator in async context
                        needs_conf = self._func_needs_config(node_func)
                        args = (node_input, task_config) if needs_conf else (node_input,)
                        gen = node_func(*args)
                        result = None
                        try:
                            while True:
                                chunk = next(gen)
                                yield ("messages", {"node": node_name, "chunk": chunk}) if is_multi else {"node": node_name, "chunk": chunk}
                        except StopIteration as e:
                            result = e.value
                    else:
                        result = await self._acall_node(
                            node_func, node_input,
                            retry_policy=node_spec.retry_policy,
                            timeout=node_spec.timeout.run_timeout if node_spec.timeout else None,
                        )
                except GraphInterrupt as gi:
                    _current_config.reset(token)
                    checkpoint = self._checkpoint_from_channels(channels, checkpoint)
                    checkpoint["_next_nodes"] = (node_name,)
                    for interrupt_val in gi.interrupts:
                        self._save_interrupt(config, checkpoint, interrupt_val)
                    if "values" in modes:
                        val = self._read_output(channels)
                        yield ("values", val) if is_multi else val
                    return
                except GraphBubbleUp:
                    _current_config.reset(token)
                    raise
                except Exception as e:
                    _current_config.reset(token)
                    if node_spec.error_handler_node:
                        error_val = {"node": node_name, "error": str(e)}
                        if "__error__" in channels:
                            channels["__error__"].update([error_val])
                        new_next.add(node_spec.error_handler_node)
                        completed_nodes.add(node_name)
                        if "tasks" in modes:
                            yield ("tasks", {"type": "task_end", "step": step, "node": node_name}) if is_multi else {"type": "task_end", "step": step, "node": node_name}
                        continue
                    raise

                _current_config.reset(token)
                completed_nodes.add(node_name)

                if "tasks" in modes:
                    yield ("tasks", {"type": "task_end", "step": step, "node": node_name}) if is_multi else {"type": "task_end", "step": step, "node": node_name}

                # Store result in cache
                if cache_key is not None and result is not None:
                    self._store_cache(cache_key, node_spec.cache_policy, result)

                # Yield custom writer events
                if "custom" in modes and custom_events:
                    for cn, cv in custom_events:
                        yield ("custom", {"node": cn, "value": cv}) if is_multi else {"node": cn, "value": cv}

                if result is not None:
                    result_updates = self._process_result(result, channels)
                    if result_updates and "updates" in modes:
                        updates[node_name] = result_updates

                    if node_name not in builder.branches:
                        if isinstance(result, Send):
                            new_sends.append(result)
                        elif isinstance(result, (list, tuple)):
                            for item in result:
                                if isinstance(item, Send):
                                    new_sends.append(item)

                if "debug" in modes:
                    yield ("debug", {"type": "task_result", "step": step, "node": node_name, "result": result}) if is_multi else {"type": "task_result", "step": step, "node": node_name, "result": result}

                node_next, extra_sends = self._get_next_nodes(builder, node_name, result, channels)
                new_next.update(node_next)
                new_sends.extend(extra_sends)

            # Process new sends (with nested Send support)
            i = 0
            while i < len(new_sends):
                send = new_sends[i]
                i += 1
                if send.node in builder.nodes:
                    node_spec = builder.nodes[send.node]
                    send_scratchpad = Scratchpad(
                        step=step, stop=recursion_limit,
                        resume=[resume_value] if resume_value is not None else None
                    )
                    send_custom_events: list = []
                    send_custom_writer = None
                    if "custom" in modes:
                        def _make_send_writer(n):
                            def writer(value):
                                send_custom_events.append((n, value))
                            return writer
                        send_custom_writer = _make_send_writer(send.node)
                    send_config = self._make_task_config(
                        config, send.node, channels, send_scratchpad,
                        custom_writer=send_custom_writer,
                    )
                    send_token = _current_config.set(send_config)
                    try:
                        cache_key = self._get_cache_key(
                            send.node, send.arg, node_spec.cache_policy
                        )
                        cached = self._check_cache(cache_key, node_spec.cache_policy)
                        if cached is not None:
                            result = cached
                        else:
                            result = await self._acall_node(
                                node_spec.runnable, send.arg,
                                retry_policy=node_spec.retry_policy,
                                timeout=node_spec.timeout.run_timeout if node_spec.timeout else None,
                            )
                            if cache_key is not None and result is not None:
                                self._store_cache(cache_key, node_spec.cache_policy, result)
                    except GraphBubbleUp:
                        _current_config.reset(send_token)
                        raise
                    except Exception as send_err:
                        _current_config.reset(send_token)
                        if node_spec.error_handler_node:
                            error_val = {"node": send.node, "error": str(send_err)}
                            if "__error__" in channels:
                                channels["__error__"].update([error_val])
                            new_next.add(node_spec.error_handler_node)
                            completed_nodes.add(send.node)
                            continue
                        raise
                    _current_config.reset(send_token)

                    if "custom" in modes and send_custom_events:
                        for cn, cv in send_custom_events:
                            yield ("custom", {"node": cn, "value": cv}) if is_multi else {"node": cn, "value": cv}

                    if result is not None:
                        result_updates = self._process_result(result, channels)
                        if result_updates and "updates" in modes:
                            updates.setdefault(send.node, {}).update(result_updates)
                        if isinstance(result, Send):
                            new_sends.append(result)
                        elif isinstance(result, (list, tuple)):
                            for item in result:
                                if isinstance(item, Send):
                                    new_sends.append(item)
                    completed_nodes.add(send.node)
                    node_next, extra_sends = self._get_next_nodes(builder, send.node, result, channels)
                    new_next.update(node_next)
                    new_sends.extend(extra_sends)

            # Check interrupt_after
            if self.interrupt_after:
                nodes_to_interrupt = [
                    n for n in next_nodes
                    if ("*" in self.interrupt_after or n in self.interrupt_after)
                    and n in builder.nodes
                ]
                if nodes_to_interrupt:
                    checkpoint = self._checkpoint_from_channels(channels, checkpoint)
                    checkpoint["_next_nodes"] = list(new_next)
                    self._save_checkpoint(config, checkpoint)
                    if "values" in modes:
                        val = self._read_output(channels)
                        yield ("values", val) if is_multi else val
                    if "updates" in modes:
                        if updates:
                            yield ("updates", updates) if is_multi else updates
                    return

            # Save checkpoint
            checkpoint = self._checkpoint_from_channels(channels, checkpoint)
            checkpoint["_next_nodes"] = list(new_next)
            checkpoint["_step"] = step + 1
            self._save_checkpoint(config, checkpoint)

            if "checkpoints" in modes:
                cp_event = {"step": step, "next_nodes": list(new_next), "checkpoint_id": checkpoint.get("id")}
                yield ("checkpoints", cp_event) if is_multi else cp_event
            if "values" in modes:
                last_event = self._read_output(channels)
                yield ("values", last_event) if is_multi else last_event
            if "updates" in modes:
                if updates:
                    yield ("updates", updates) if is_multi else updates
            if "debug" in modes:
                dbg_ev = {"type": "step_end", "step": step, "next_nodes": list(new_next)}
                yield ("debug", dbg_ev) if is_multi else dbg_ev

            # After executing all nodes, check waiting edges for next step
            waiting_next = set()
            for starts, end in builder.waiting_edges:
                if (end not in new_next
                    and end not in completed_nodes
                    and all(s in completed_nodes for s in starts)):
                    if end != END:
                        waiting_next.add(end)

            next_nodes = list(new_next | waiting_next)

        if "values" in modes and last_event is None:
            val = self._read_output(channels)
            yield ("values", val) if is_multi else val

    # ---- Parallel async execution ----

    async def _arun_parallel_step(
        self,
        next_nodes: list[str],
        channels: dict,
        config: dict,
        modes: set[str],
        is_multi: bool,
        step: int,
        recursion_limit: int,
        resume_value,
        max_concurrency: int,
    ) -> tuple[dict, set[str], list[Send], list, dict]:
        """Run multiple nodes in parallel with concurrency control.

        Returns (updates, new_next, new_sends, buffered_events, results_by_node).
        Each buffered_event is (mode, data) for multi-mode or (None, data) for single-mode.
        """
        builder = self.builder
        sem = asyncio.Semaphore(max_concurrency)

        # Collect which nodes to run (skip unknown nodes)
        runnable_nodes = [n for n in next_nodes if n in builder.nodes]

        async def run_single(node_name: str) -> dict:
            """Execute one node and return result struct."""
            node_spec = builder.nodes[node_name]
            node_func = node_spec.runnable
            result_info = {
                "node": node_name,
                "result": None,
                "updates": {},
                "new_next": set(),
                "new_sends": [],
                "error": None,
                "custom_events": [],
            }

            async with sem:
                # Read input — snapshot the channel values to avoid concurrent mutation
                try:
                    node_input = self._read_node_input(channels, node_spec, builder)
                except EmptyChannelError:
                    return result_info
                except Exception:
                    if node_spec.error_handler_node:
                        result_info["error"] = "input read failed"
                        result_info["new_next"] = {node_spec.error_handler_node}
                        return result_info
                    raise

                # Check cache
                cache_key = self._get_cache_key(node_name, node_input, node_spec.cache_policy)
                cached_result = self._check_cache(cache_key, node_spec.cache_policy)
                if cached_result is not None:
                    result_info["result"] = cached_result
                    node_next, extra_sends = self._get_next_nodes(builder, node_name, cached_result, channels)
                    result_info["new_next"] = node_next
                    result_info["new_sends"] = extra_sends
                    return result_info

                scratchpad = Scratchpad(
                    step=step, stop=recursion_limit,
                    resume=[resume_value] if resume_value is not None else None
                )
                custom_events_list: list = []

                if "custom" in modes:
                    def _make_writer(n, evts):
                        def writer(value):
                            evts.append((n, value))
                        return writer
                    custom_writer = _make_writer(node_name, custom_events_list)
                else:
                    custom_writer = None

                task_config = self._make_task_config(
                    config, node_name, channels, scratchpad,
                    custom_writer=custom_writer,
                )

                token = _current_config.set(task_config)
                try:
                    result = await self._acall_node(
                        node_func, node_input,
                        retry_policy=node_spec.retry_policy,
                        timeout=node_spec.timeout.run_timeout if node_spec.timeout else None,
                    )
                except GraphInterrupt:
                    _current_config.reset(token)
                    result_info["error"] = "interrupt"
                    return result_info
                except Exception as e:
                    _current_config.reset(token)
                    if node_spec.error_handler_node:
                        result_info["error_handler_node"] = node_spec.error_handler_node
                        result_info["error_val"] = {"node": node_name, "error": str(e)}
                        result_info["new_next"].add(node_spec.error_handler_node)
                        return result_info
                    result_info["error"] = e
                    return result_info

                _current_config.reset(token)

                # Store cache
                if cache_key is not None and result is not None:
                    self._store_cache(cache_key, node_spec.cache_policy, result)

                result_info["result"] = result
                result_info["custom_events"] = custom_events_list

                if result is not None:
                    if isinstance(result, Send):
                        result_info["new_sends"].append(result)
                    elif isinstance(result, (list, tuple)):
                        for item in result:
                            if isinstance(item, Send):
                                result_info["new_sends"].append(item)

                node_next, extra_sends = self._get_next_nodes(builder, node_name, result, channels)
                result_info["new_next"] = node_next
                result_info["new_sends"].extend(extra_sends)
                return result_info

        # Run all nodes concurrently
        tasks = [asyncio.create_task(run_single(n)) for n in runnable_nodes]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        # Process results sequentially — apply to shared channels here
        all_updates = {}
        all_new_next: set[str] = set()
        all_new_sends: list[Send] = []
        all_events: list = []
        results_by_node: dict = {}

        for r in results:
            if isinstance(r, Exception):
                raise r

            node_name = r["node"]
            results_by_node[node_name] = r

            # Task events
            if "tasks" in modes:
                all_events.append(("tasks", {"type": "task_start", "step": step, "node": node_name}))

            # Apply error handler writes to channels
            if r.get("error_handler_node") and r.get("error_val"):
                if "__error__" in channels:
                    channels["__error__"].update([r["error_val"]])

            # Apply result to channels
            if r["result"] is not None and r["error"] is None:
                try:
                    result_updates = self._process_result(r["result"], channels)
                except GraphBubbleUp:
                    raise
                if result_updates and "updates" in modes:
                    all_updates[node_name] = result_updates

            # Custom events
            if "custom" in modes and r.get("custom_events"):
                for cn, cv in r["custom_events"]:
                    all_events.append(("custom", {"node": cn, "value": cv}))

            # Task end event
            if "tasks" in modes:
                all_events.append(("tasks", {"type": "task_end", "step": step, "node": node_name}))

            all_new_next.update(r["new_next"])
            all_new_sends.extend(r["new_sends"])

        return all_updates, all_new_next, all_new_sends, all_events, results_by_node

    # ---- Node execution helpers ----

    def _call_node(self, func: Callable, node_input: Any,
                    retry_policy: RetryPolicy | None = None,
                    timeout: float | None = None) -> Any:
        # Handle subgraph (CompiledStateGraph as node function)
        if hasattr(func, '_loop') and hasattr(func, 'builder'):
            return self._execute_subgraph(func, node_input)

        if timeout is not None and timeout > 0:
            return self._call_with_timeout(func, node_input, retry_policy, timeout)

        if retry_policy is None:
            return self._execute_func(func, node_input)

        import random
        max_attempts = retry_policy.max_attempts
        for attempt in range(max_attempts):
            try:
                return self._execute_func(func, node_input)
            except Exception as e:
                retry_on = retry_policy.retry_on
                should_retry = False
                if isinstance(retry_on, type):
                    should_retry = isinstance(e, retry_on)
                elif isinstance(retry_on, (list, tuple)):
                    should_retry = isinstance(e, tuple(retry_on))
                elif callable(retry_on):
                    should_retry = retry_on(e)
                else:
                    should_retry = True

                if not should_retry or attempt >= max_attempts - 1:
                    raise

                interval = retry_policy.initial_interval * (retry_policy.backoff_factor ** attempt)
                interval = min(interval, retry_policy.max_interval)
                if retry_policy.jitter:
                    interval *= random.uniform(0.5, 1.5)
                time.sleep(interval)

    def _call_with_timeout(self, func: Callable, node_input: Any,
                           retry_policy: RetryPolicy | None,
                           timeout_seconds: float) -> Any:
        """Execute a node function with a timeout."""
        import concurrent.futures
        if retry_policy is None:
            with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
                future = pool.submit(self._execute_func, func, node_input)
                try:
                    return future.result(timeout=timeout_seconds)
                except concurrent.futures.TimeoutError:
                    raise TimeoutError(
                        f"Node timed out after {timeout_seconds}s"
                    )

        import random
        max_attempts = retry_policy.max_attempts
        for attempt in range(max_attempts):
            with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
                future = pool.submit(self._execute_func, func, node_input)
                try:
                    return future.result(timeout=timeout_seconds)
                except concurrent.futures.TimeoutError:
                    if attempt >= max_attempts - 1:
                        raise TimeoutError(
                            f"Node timed out after {timeout_seconds}s "
                            f"(attempt {attempt + 1}/{max_attempts})"
                        )
            interval = retry_policy.initial_interval * (retry_policy.backoff_factor ** attempt)
            interval = min(interval, retry_policy.max_interval)
            if retry_policy.jitter:
                interval *= random.uniform(0.5, 1.5)
            time.sleep(interval)

    def _execute_func(self, func: Callable, node_input: Any) -> Any:
        config = _current_config.get({})
        needs_config = self._func_needs_config(func)
        if asyncio.iscoroutinefunction(func):
            try:
                loop = asyncio.get_running_loop()
            except RuntimeError:
                loop = None
            if loop and loop.is_running():
                import concurrent.futures
                with concurrent.futures.ThreadPoolExecutor() as pool:
                    args = (node_input, config) if needs_config else (node_input,)
                    future = pool.submit(asyncio.run, func(*args))
                    return future.result()
            else:
                args = (node_input, config) if needs_config else (node_input,)
                return asyncio.run(func(*args))
        else:
            args = (node_input, config) if needs_config else (node_input,)
            return func(*args)

    async def _acall_node(self, func: Callable, node_input: Any,
                          retry_policy: RetryPolicy | None = None,
                          timeout: float | None = None) -> Any:
        """Async version of _call_node with retry and timeout support."""
        # Handle subgraph (CompiledStateGraph as node function)
        if hasattr(func, '_loop') and hasattr(func, 'builder'):
            return await self._aexecute_subgraph(func, node_input)

        if timeout is not None and timeout > 0:
            return await asyncio.wait_for(
                self._acall_node_inner(func, node_input, retry_policy),
                timeout=timeout,
            )

        return await self._acall_node_inner(func, node_input, retry_policy)

    async def _acall_node_inner(self, func: Callable, node_input: Any,
                                retry_policy: RetryPolicy | None = None) -> Any:
        if retry_policy is None:
            return await self._aexecute_func(func, node_input)

        import random
        max_attempts = retry_policy.max_attempts
        for attempt in range(max_attempts):
            try:
                return await self._aexecute_func(func, node_input)
            except Exception as e:
                retry_on = retry_policy.retry_on
                should_retry = False
                if isinstance(retry_on, type):
                    should_retry = isinstance(e, retry_on)
                elif isinstance(retry_on, (list, tuple)):
                    should_retry = isinstance(e, tuple(retry_on))
                elif callable(retry_on):
                    should_retry = retry_on(e)
                else:
                    should_retry = True

                if not should_retry or attempt >= max_attempts - 1:
                    raise

                interval = retry_policy.initial_interval * (retry_policy.backoff_factor ** attempt)
                interval = min(interval, retry_policy.max_interval)
                if retry_policy.jitter:
                    interval *= random.uniform(0.5, 1.5)
                await asyncio.sleep(interval)

    async def _aexecute_func(self, func: Callable, node_input: Any) -> Any:
        """Execute a function, awaiting if async."""
        config = _current_config.get({})
        needs_config = self._func_needs_config(func)
        args = (node_input, config) if needs_config else (node_input,)
        if asyncio.iscoroutinefunction(func):
            return await func(*args)
        else:
            return func(*args)

    @staticmethod
    def _func_needs_config(func: Callable) -> bool:
        """Check if a function expects a config argument."""
        try:
            sig = inspect.signature(func)
            params = list(sig.parameters.values())
            # If 2+ positional params, assume (state, config)
            positional = sum(
                1 for p in params
                if p.kind in (p.POSITIONAL_ONLY, p.POSITIONAL_OR_KEYWORD)
            )
            return positional >= 2
        except (ValueError, TypeError):
            return False

    def _execute_subgraph(self, subgraph: Any, node_input: Any) -> Any:
        """Execute a CompiledStateGraph as a subgraph node."""
        from zerograph.constants import NS_SEP, CONFIG_KEY_CHECKPOINTER
        config = _current_config.get({}).copy()
        config.setdefault("configurable", {})
        if self.store is not None:
            config["configurable"][CONFIG_KEY_STORE] = self.store
        if self.cache is not None:
            config["configurable"][CONFIG_KEY_CACHE] = self.cache
        if self.checkpointer is not None:
            config["configurable"][CONFIG_KEY_CHECKPOINTER] = self.checkpointer
        parent_ns = config["configurable"].get(CONFIG_KEY_CHECKPOINT_NS, "")
        sub_ns = self._infer_subgraph_ns(config, parent_ns)
        config["configurable"][CONFIG_KEY_CHECKPOINT_NS] = sub_ns

        # Check if the parent is resuming from an interrupt
        scratchpad = config.get("configurable", {}).get("__scratchpad__")
        parent_resume = None
        if scratchpad is not None and scratchpad.resume:
            parent_resume = scratchpad.resume[0]

        if parent_resume is not None:
            # Pass resume value to subgraph as a Command so the subgraph's
            # _run knows to resume from its interrupted checkpoint.
            # Also clear checkpoint_id so the subgraph loads its own latest
            # checkpoint instead of looking for the parent's checkpoint_id.
            sub_input = Command(resume=parent_resume)
            config["configurable"].pop("checkpoint_id", None)
        else:
            sub_input = self._map_subgraph_input(subgraph, node_input)

        effective_checkpointer = self.checkpointer or config.get("configurable", {}).get(CONFIG_KEY_CHECKPOINTER)

        saved_cp = subgraph._loop.checkpointer
        if effective_checkpointer is not None and subgraph._loop.checkpointer is None:
            subgraph._loop.checkpointer = effective_checkpointer

        try:
            result = subgraph.invoke(sub_input, config)
        except ParentCommand:
            subgraph._loop.checkpointer = saved_cp
            raise

        subgraph._loop.checkpointer = saved_cp

        if effective_checkpointer is not None:
            sub_config = {
                "configurable": {
                    **config.get("configurable", {}),
                    CONFIG_KEY_CHECKPOINT_NS: sub_ns,
                }
            }
            sub_interrupts = []
            if hasattr(effective_checkpointer, "get_pending_writes"):
                pw = effective_checkpointer.get_pending_writes(sub_config)
                for tid, ch, val in pw:
                    if ch == INTERRUPT and isinstance(val, Interrupt):
                        sub_interrupts.append(val)

            if sub_interrupts:
                raise GraphInterrupt(sub_interrupts)

            self._save_subgraph_state_with(subgraph, sub_config, result, effective_checkpointer)

        return result

    async def _aexecute_subgraph(self, subgraph: Any, node_input: Any) -> Any:
        """Execute a CompiledStateGraph as a subgraph node (async)."""
        from zerograph.constants import NS_SEP, CONFIG_KEY_CHECKPOINTER
        config = _current_config.get({}).copy()
        config.setdefault("configurable", {})
        if self.store is not None:
            config["configurable"][CONFIG_KEY_STORE] = self.store
        if self.cache is not None:
            config["configurable"][CONFIG_KEY_CACHE] = self.cache
        if self.checkpointer is not None:
            config["configurable"][CONFIG_KEY_CHECKPOINTER] = self.checkpointer
        parent_ns = config["configurable"].get(CONFIG_KEY_CHECKPOINT_NS, "")
        sub_ns = self._infer_subgraph_ns(config, parent_ns)
        config["configurable"][CONFIG_KEY_CHECKPOINT_NS] = sub_ns

        scratchpad = config.get("configurable", {}).get("__scratchpad__")
        parent_resume = None
        if scratchpad is not None and scratchpad.resume:
            parent_resume = scratchpad.resume[0]

        if parent_resume is not None:
            sub_input = Command(resume=parent_resume)
            config["configurable"].pop("checkpoint_id", None)
        else:
            sub_input = self._map_subgraph_input(subgraph, node_input)

        effective_checkpointer = self.checkpointer or config.get("configurable", {}).get(CONFIG_KEY_CHECKPOINTER)

        saved_cp = subgraph._loop.checkpointer
        if effective_checkpointer is not None and subgraph._loop.checkpointer is None:
            subgraph._loop.checkpointer = effective_checkpointer

        try:
            result = await subgraph.ainvoke(sub_input, config)
        except ParentCommand:
            subgraph._loop.checkpointer = saved_cp
            raise

        subgraph._loop.checkpointer = saved_cp

        if effective_checkpointer is not None:
            sub_config = {
                "configurable": {
                    **config.get("configurable", {}),
                    CONFIG_KEY_CHECKPOINT_NS: sub_ns,
                }
            }
            sub_interrupts = []
            if hasattr(effective_checkpointer, "get_pending_writes"):
                pw = effective_checkpointer.get_pending_writes(sub_config)
                for tid, ch, val in pw:
                    if ch == INTERRUPT and isinstance(val, Interrupt):
                        sub_interrupts.append(val)

            if sub_interrupts:
                raise GraphInterrupt(sub_interrupts)

            self._save_subgraph_state_with(subgraph, sub_config, result, effective_checkpointer)

        return result

    def _save_subgraph_state(self, subgraph: Any, sub_config: dict, result: Any) -> None:
        """Save subgraph result as a checkpoint under subgraph namespace."""
        if self.checkpointer is not None:
            self._save_subgraph_state_with(subgraph, sub_config, result, self.checkpointer)

    @staticmethod
    def _save_subgraph_state_with(subgraph: Any, sub_config: dict, result: Any, checkpointer: Any) -> None:
        """Save subgraph result as a checkpoint under subgraph namespace."""
        import uuid
        from datetime import datetime, timezone
        sub_cp = {
            "v": 1,
            "id": str(uuid.uuid4()),
            "ts": datetime.now(timezone.utc).isoformat(),
            "channel_values": result if isinstance(result, dict) else {"__root__": result},
            "channel_versions": {},
            "versions_seen": {},
            "_next_nodes": [],
            "_step": 1,
        }
        meta = CheckpointMetadata(source="subgraph", step=1)
        checkpointer.put(sub_config, sub_cp, meta)

    @staticmethod
    def _map_subgraph_input(subgraph: Any, node_input: Any) -> Any:
        """Map parent state to subgraph input based on subgraph's schema."""
        if not isinstance(node_input, dict):
            return node_input
        # Get subgraph's input channels
        builder = subgraph.builder
        sub_channels = builder.schemas.get(builder.input_schema, builder.channels)
        sub_keys = set(sub_channels.keys())
        if "__root__" in sub_keys:
            return node_input
        # Filter parent input to only include keys the subgraph expects
        mapped = {k: v for k, v in node_input.items() if k in sub_keys}
        return mapped

    def _infer_subgraph_ns(self, config: dict, parent_ns: str) -> str:
        """Build checkpoint namespace for a subgraph node."""
        from zerograph.constants import NS_SEP
        configurable = config.get("configurable", {})
        node_name = configurable.get("__node_name__", "subgraph")
        if parent_ns:
            return parent_ns + NS_SEP + node_name
        return node_name

    def _read_node_input(self, channels: dict, node_spec, builder) -> Any:
        """Read input for a node based on its input schema."""
        input_schema = node_spec.input_schema or builder.state_schema
        schema_channels = builder.schemas.get(input_schema, builder.channels)

        if "__root__" in schema_channels and len(schema_channels) == 1:
            return channels["__root__"].get()

        result = {}
        for key in schema_channels:
            if key in channels and channels[key].is_available():
                result[key] = channels[key].get()
        return result

    def _process_result(self, result: Any, channels: dict) -> dict | None:
        """Process node output, apply to channels. Returns the update dict."""
        updates = {}

        if isinstance(result, Send):
            return None
        elif isinstance(result, Command):
            # Command.PARENT: bubble up to parent graph
            if result.graph == Command.PARENT:
                raise ParentCommand(result)
            if result.update:
                tuples = result._update_as_tuples()
                updates = {k: v for k, v in tuples}
        elif isinstance(result, dict):
            updates = result
        elif isinstance(result, (list, tuple)):
            has_sends = any(isinstance(i, Send) for i in result)
            if has_sends:
                for item in result:
                    if isinstance(item, dict) and not isinstance(item, (Send, Command)):
                        updates.update(item)
                if not updates:
                    return None
            else:
                return None
        elif result is not None and "__root__" in channels:
            updates = {"__root__": result}
        else:
            return None

        # Apply updates to channels
        for key, val in updates.items():
            if key in channels:
                if isinstance(val, Overwrite):
                    ch = channels[key]
                    if isinstance(ch, BinaryOperatorAggregate):
                        ch.update([Overwrite(value=val.value)])
                    else:
                        ch.update([val.value])
                else:
                    channels[key].update([val])

        return {k: v for k, v in updates.items() if not isinstance(v, Overwrite)}

    def _get_start_nodes(self, checkpoint: dict, is_resuming: bool,
                         channels: dict = None) -> tuple[list[str], list[Send]]:
        """Get the first nodes to execute and any Send objects from START branches."""
        builder = self.builder
        sends: list[Send] = []

        if is_resuming and "_next_nodes" in checkpoint:
            saved_next = checkpoint["_next_nodes"]
            return list(saved_next), []

        start_nodes = set()

        # Direct edges from START
        for start, end in builder.edges:
            if start == START:
                start_nodes.add(end)

        # Conditional edges from START
        if START in builder.branches:
            for name, branch in builder.branches[START].items():
                if channels is not None:
                    try:
                        input_val = self._read_output(channels)
                        path_result = branch.path(input_val)
                    except Exception:
                        path_result = None
                else:
                    path_result = None

                if path_result is not None:
                    if not isinstance(path_result, (list, tuple)):
                        path_result = [path_result]

                    for dest in path_result:
                        if isinstance(dest, Send):
                            sends.append(dest)
                        elif dest == END:
                            pass
                        elif branch.ends and dest in branch.ends:
                            start_nodes.add(branch.ends[dest])
                        elif isinstance(dest, str) and dest in builder.nodes:
                            start_nodes.add(dest)
                elif branch.ends:
                    # No result but have path_map -> add all mapped ends
                    start_nodes.update(branch.ends.values())
                # FIX: removed the dangerous fallback that added ALL nodes
                # When path_result is None and no branch.ends, do nothing
                # (the path function failed to produce a result)

        return list(start_nodes), sends

    def _get_next_nodes(self, builder, node_name: str, result: Any,
                        channels: dict) -> tuple[set[str], list[Send]]:
        """Determine which nodes should run after the given node.
        Returns (next_node_names, sends_to_process)."""
        # Command.goto overrides normal edge routing
        if isinstance(result, Command) and result.goto:
            next_nodes = set()
            sends: list[Send] = []
            goto = result.goto
            if isinstance(goto, (list, tuple)):
                for g in goto:
                    if isinstance(g, Send):
                        sends.append(g)
                    elif isinstance(g, str) and g != END:
                        next_nodes.add(g)
            elif isinstance(goto, Send):
                sends.append(goto)
            elif isinstance(goto, str) and goto != END:
                next_nodes.add(goto)
            return next_nodes, sends

        next_nodes = set()
        sends = []

        # Check direct edges
        for start, end in builder.edges:
            if start == node_name and end != END:
                next_nodes.add(end)

        # Check conditional edges
        if node_name in builder.branches:
            for name, branch in builder.branches[node_name].items():
                try:
                    # If result is already a routing decision (list of
                    # Send / str), use it directly instead of re-invoking
                    # the router function, which would produce duplicates.
                    if isinstance(result, (list, tuple)):
                        has_sends_or_strs = any(
                            isinstance(r, (Send, str)) for r in result
                        )
                        if has_sends_or_strs:
                            path_result = result
                        else:
                            path_result = branch.path(
                                result if isinstance(result, dict)
                                else self._read_output(channels)
                            )
                    else:
                        path_result = branch.path(
                            result if isinstance(result, dict)
                            else self._read_output(channels)
                        )
                except Exception:
                    continue

                if not isinstance(path_result, (list, tuple)):
                    path_result = [path_result]

                for dest in path_result:
                    if isinstance(dest, Send):
                        sends.append(dest)
                    elif dest == END:
                        pass
                    elif branch.ends and dest in branch.ends:
                        next_nodes.add(branch.ends[dest])
                    elif dest in builder.nodes:
                        next_nodes.add(dest)

        return next_nodes, sends

    # ---- Channel helpers ----

    def _apply_input(self, input: Any, channels: dict) -> None:
        if isinstance(input, dict):
            for key, val in input.items():
                if key in channels:
                    channels[key].update([val])
        elif "__root__" in channels:
            channels["__root__"].update([input])

    def _read_output(self, channels: dict) -> dict[str, Any] | Any:
        """Read output from channels."""
        if isinstance(self.output_channels, str):
            ch = self.output_channels
            if ch in channels and channels[ch].is_available():
                return channels[ch].get()
            return {}

        result = {}
        for key in self.output_channels:
            if key in channels and channels[key].is_available():
                result[key] = channels[key].get()
        return result

    def _make_task_config(self, config: dict, node_name: str,
                          channels: dict, scratchpad,
                          custom_writer: Any = None) -> dict:
        task_config = copy.deepcopy(config)
        writes_list = []
        configurable = {
            **config.get("configurable", {}),
            CONFIG_KEY_TASK_ID: str(uuid.uuid4()),
            CONFIG_KEY_SEND: lambda w: writes_list.extend(w),
            CONFIG_KEY_READ: lambda select, fresh=False: self._local_read(
                channels, writes_list, select, fresh
            ),
            "__scratchpad__": scratchpad,
            "__node_name__": node_name,
        }
        # Inject store
        if self.store is not None:
            configurable[CONFIG_KEY_STORE] = self.store
        # Inject cache
        if self.cache is not None:
            configurable[CONFIG_KEY_CACHE] = self.cache
        # Inject context
        if hasattr(self.builder, 'context_schema') and self.builder.context_schema:
            ctx = {}
            for key in self.builder.schemas.get(self.builder.context_schema, {}):
                if key in channels and channels[key].is_available():
                    ctx[key] = channels[key].get()
            configurable[CONFIG_KEY_CONTEXT] = ctx
        # Inject custom writer for "custom" stream mode
        if custom_writer is not None:
            configurable[CONFIG_KEY_WRITER] = custom_writer
        task_config["configurable"] = configurable
        return task_config

    def _local_read(self, channels, writes, select, fresh=False):
        if isinstance(select, str):
            return channels[select].get() if select in channels and channels[select].is_available() else None
        result = {}
        for k in select:
            if k in channels and channels[k].is_available():
                result[k] = channels[k].get()
        return result

    def _apply_context(self, channels: dict) -> None:
        """Apply context values to channels (immutable runtime context)."""
        if self.context is None:
            return
        for key, val in self.context.items():
            if key in channels:
                channels[key].update([val])

    # ---- Cache helpers ----

    def _get_cache_key(self, node_name: str, node_input: Any,
                       cache_policy: Any) -> str | None:
        if cache_policy is None or self.cache is None:
            return None
        if cache_policy.key_func is not None:
            return cache_policy.key_func(node_name, node_input)
        return f"{node_name}:{hash(repr(node_input))}"

    def _check_cache(self, cache_key: str | None,
                     cache_policy: Any) -> Any | None:
        if cache_key is None or self.cache is None:
            return None
        return self.cache.get(cache_key)

    def _store_cache(self, cache_key: str | None,
                     cache_policy: Any, result: Any) -> None:
        if cache_key is None or self.cache is None:
            return
        ttl = cache_policy.ttl if cache_policy else None
        self.cache.set(cache_key, result, ttl=ttl)

    # ---- Checkpoint helpers ----

    def _create_empty_checkpoint(self) -> dict:
        return {
            "v": 1,
            "id": str(uuid.uuid4()),
            "ts": datetime.now(timezone.utc).isoformat(),
            "channel_values": {},
            "channel_versions": {},
            "versions_seen": {},
            "_step": 0,
        }

    def _checkpoint_from_channels(self, channels: dict, base: dict) -> dict:
        cv = {}
        for name, ch in channels.items():
            try:
                val = ch.checkpoint()
                if val is not MISSING:
                    cv[name] = val
            except EmptyChannelError:
                pass
        cp = copy.deepcopy(base)
        cp["channel_values"] = cv
        return cp

    def _restore_channels(self, checkpoint: dict) -> dict[str, BaseChannel]:
        cv = checkpoint.get("channel_values", {})
        channels = {}
        for name, ch in self.channels.items():
            val = cv.get(name, MISSING)
            channels[name] = ch.from_checkpoint(val)
        return channels

    def _load_checkpoint(self, config: dict) -> dict | None:
        if self.checkpointer is None:
            return None
        tup = self.checkpointer.get_tuple(config)
        if tup:
            cp = dict(tup.checkpoint)
            cp["_metadata"] = tup.metadata
            return cp
        return None

    def _save_checkpoint(self, config: dict, checkpoint: dict) -> dict:
        if self.checkpointer is None:
            return config
        metadata = checkpoint.pop("_metadata", CheckpointMetadata(source="input", step=0))
        # Generate new id and ts for each save to keep history
        checkpoint["id"] = str(uuid.uuid4())
        checkpoint["ts"] = datetime.now(timezone.utc).isoformat()
        new_config = self.checkpointer.put(config, checkpoint, metadata)
        # Update config in place so subsequent operations use the correct checkpoint_id
        config.setdefault("configurable", {})["checkpoint_id"] = new_config.get("configurable", {}).get("checkpoint_id", checkpoint["id"])
        return new_config

    def _get_pending_writes(self, config: dict) -> list:
        if self.checkpointer is None:
            return []
        if hasattr(self.checkpointer, "get_pending_writes"):
            return self.checkpointer.get_pending_writes(config)
        return []

    def _save_interrupt(self, config, checkpoint, interrupt_val):
        if self.checkpointer is not None:
            checkpoint["_metadata"] = CheckpointMetadata(source="interrupt")
            self._save_checkpoint(config, checkpoint)
            if hasattr(self.checkpointer, "put_writes"):
                self.checkpointer.put_writes(
                    config,
                    [(NULL_TASK_ID, INTERRUPT, interrupt_val)],
                    NULL_TASK_ID,
                )
