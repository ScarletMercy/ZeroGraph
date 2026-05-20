"""Pregel algorithm core - task scheduling and write application."""

from __future__ import annotations

import itertools
import threading
from collections import defaultdict, deque
from collections.abc import Mapping, Sequence
from copy import copy
from hashlib import sha256
from typing import Any, NamedTuple

from zerograph._internal import EMPTY_SEQ, MISSING
from zerograph.channels.base import BaseChannel
from zerograph.channels.topic import Topic
from zerograph.constants import (
    END,
    ERROR,
    ERROR_SOURCE_NODE,
    INTERRUPT,
    NO_WRITES,
    NS_END,
    NS_SEP,
    NULL_TASK_ID,
    PULL,
    PUSH,
    RESERVED,
    RESUME,
    RETURN,
    START,
    TAG_HIDDEN,
    TASKS,
)
from zerograph.errors import EmptyChannelError, GraphBubbleUp, GraphInterrupt
from zerograph.types import Command, Interrupt, PregelTask, Send

__all__ = (
    "increment",
    "apply_writes",
    "prepare_next_tasks",
    "should_interrupt",
    "local_read",
    "read_channels",
)


def increment(current: int | None, _: None) -> int:
    return current + 1 if current is not None else 1


def read_channels(
    channels: Mapping[str, BaseChannel],
    select: list[str] | str,
) -> dict[str, Any] | Any:
    """Read values from channels."""
    if isinstance(select, str):
        return channels[select].get()
    values: dict[str, Any] = {}
    for k in select:
        if k in channels and channels[k].is_available():
            values[k] = channels[k].get()
    return values


def should_interrupt(
    checkpoint: dict,
    interrupt_nodes: list[str] | str,
    tasks: list,
) -> list:
    """Check if the graph should be interrupted."""
    versions = checkpoint.get("channel_versions", {})
    seen = checkpoint.get("versions_seen", {}).get(INTERRUPT, {})
    null_version = 0

    any_updates = any(
        versions.get(chan, null_version) > seen.get(chan, null_version)
        for chan in versions
    )
    if not any_updates:
        return []

    if isinstance(interrupt_nodes, str):
        interrupt_list = [interrupt_nodes]
    else:
        interrupt_list = list(interrupt_nodes)
    if "*" in interrupt_list:
        return list(tasks)
    return [
        task for task in tasks
        if task.name in interrupt_list
    ]


def apply_writes(
    checkpoint: dict,
    channels: Mapping[str, BaseChannel],
    tasks: list,
    get_next_version,
    trigger_to_nodes: Mapping[str, Sequence[str]],
) -> set[str]:
    """Apply writes from tasks to channels. Returns updated channel set."""
    tasks = sorted(tasks, key=lambda t: _task_path_str(t.path[:3]))
    bump_step = any(getattr(t, 'triggers', ()) for t in tasks)

    # Update seen versions
    for task in tasks:
        versions_seen = checkpoint.setdefault("versions_seen", {})
        task_seen = versions_seen.setdefault(task.name, {})
        for chan in getattr(task, 'triggers', ()):
            if chan in checkpoint.get("channel_versions", {}):
                task_seen[chan] = checkpoint["channel_versions"][chan]

    # Find next version
    if get_next_version is None:
        next_version = None
    else:
        cv = checkpoint.get("channel_versions", {})
        next_version = get_next_version(
            max(cv.values()) if cv else None, None
        )

    # Consume trigger channels
    for chan in {
        chan
        for task in tasks
        for chan in getattr(task, 'triggers', ())
        if chan not in RESERVED and chan in channels
    }:
        if channels[chan].consume() and next_version is not None:
            checkpoint.setdefault("channel_versions", {})[chan] = next_version

    # Group writes by channel
    pending: dict[str, list] = defaultdict(list)
    for task in tasks:
        for chan, val in task.writes:
            if chan in (NO_WRITES, PUSH, RESUME, INTERRUPT, RETURN, ERROR, ERROR_SOURCE_NODE):
                pass
            elif chan in channels:
                pending[chan].append(val)

    # Apply writes
    updated_channels: set[str] = set()
    for chan, vals in pending.items():
        if chan in channels:
            if channels[chan].update(vals) and next_version is not None:
                checkpoint["channel_versions"][chan] = next_version
                if channels[chan].is_available():
                    updated_channels.add(chan)

    # Notify channels of new step
    if bump_step:
        for chan in channels:
            if channels[chan].is_available() and chan not in updated_channels:
                if channels[chan].update(EMPTY_SEQ) and next_version is not None:
                    checkpoint["channel_versions"][chan] = next_version
                    if channels[chan].is_available():
                        updated_channels.add(chan)

    # Finish channels if this is the last superstep
    if bump_step and updated_channels.isdisjoint(trigger_to_nodes):
        for chan in channels:
            if channels[chan].finish() and next_version is not None:
                checkpoint["channel_versions"][chan] = next_version
                if channels[chan].is_available():
                    updated_channels.add(chan)

    return updated_channels


class TaskWrites(NamedTuple):
    path: tuple
    name: str
    writes: list
    triggers: tuple


class ExecutableTask:
    """A task ready for execution."""

    __slots__ = (
        "name", "input", "proc", "writes", "config", "triggers",
        "id", "path", "writers",
    )

    def __init__(
        self,
        name: str,
        input: Any,
        proc: Any,
        writes: deque,
        config: dict,
        triggers: tuple,
        id: str,
        path: tuple,
        writers: list | None = None,
    ):
        self.name = name
        self.input = input
        self.proc = proc
        self.writes = writes
        self.config = config
        self.triggers = triggers
        self.id = id
        self.path = path
        self.writers = writers or []


def _task_path_str(tup) -> str:
    if isinstance(tup, (tuple, list)):
        return f"~{', '.join(_task_path_str(x) for x in tup)}"
    elif isinstance(tup, int):
        return f"{tup:010d}"
    else:
        return str(tup)


def _generate_task_id(namespace: bytes, *parts: str | bytes) -> str:
    h = sha256(namespace)
    for p in parts:
        h.update(p.encode() if isinstance(p, str) else p)
    hex_str = h.hexdigest()
    return f"{hex_str[:8]}-{hex_str[8:12]}-{hex_str[12:16]}-{hex_str[16:20]}-{hex_str[20:32]}"


def _triggers(
    channels: Mapping[str, BaseChannel],
    versions: dict,
    seen: dict | None,
    null_version: int,
    triggers: tuple,
) -> bool:
    if seen is None:
        for chan in triggers:
            if channels[chan].is_available():
                return True
    else:
        for chan in triggers:
            if channels[chan].is_available() and versions.get(chan, null_version) > seen.get(chan, null_version):
                return True
    return False


def prepare_next_tasks(
    checkpoint: dict,
    nodes: dict,
    channels: Mapping[str, BaseChannel],
    config: dict,
    step: int,
) -> dict[str, ExecutableTask]:
    """Prepare the set of tasks for the next Pregel step."""
    tasks: list[ExecutableTask] = []
    versions = checkpoint.get("channel_versions", {})
    null_version = 0

    checkpoint_id = checkpoint.get("id", "")
    try:
        checkpoint_id_bytes = bytes.fromhex(checkpoint_id.replace("-", ""))
    except (ValueError, AttributeError):
        checkpoint_id_bytes = checkpoint_id.encode()

    # Process SEND tasks (from TASKS channel)
    tasks_channel = channels.get(TASKS)
    if tasks_channel and isinstance(tasks_channel, Topic) and tasks_channel.is_available():
        sends = tasks_channel.get()
        for idx, packet in enumerate(sends):
            if not isinstance(packet, Send):
                continue
            if packet.node not in nodes:
                continue
            node_info = nodes[packet.node]
            proc = node_info.get("proc")
            if proc is None:
                continue

            triggers = (PUSH,)
            task_id = _generate_task_id(
                checkpoint_id_bytes, packet.node, str(step), PUSH, str(idx)
            )
            writes: deque = deque()
            tasks.append(ExecutableTask(
                name=packet.node,
                input=packet.arg,
                proc=proc,
                writes=writes,
                config=config,
                triggers=triggers,
                id=task_id,
                path=(PUSH, idx),
                writers=node_info.get("writers", []),
            ))

    # Process PULL tasks (nodes triggered by channel updates)
    for name, node_info in nodes.items():
        node_triggers = node_info.get("triggers", ())
        if not node_triggers:
            continue

        seen = checkpoint.get("versions_seen", {}).get(name)
        if not _triggers(channels, versions, seen, null_version, node_triggers):
            continue

        proc = node_info.get("proc")
        if proc is None:
            continue

        triggers = tuple(sorted(node_triggers))
        task_id = _generate_task_id(
            checkpoint_id_bytes, name, str(step), PULL, *triggers
        )

        # Read input from channels
        node_channels = node_info.get("channels", [])
        try:
            val = _read_proc_input(channels, node_channels)
        except EmptyChannelError:
            continue

        if val is MISSING:
            continue

        mapper = node_info.get("mapper")
        if mapper is not None:
            val = mapper(val)

        writes: deque = deque()
        tasks.append(ExecutableTask(
            name=name,
            input=val,
            proc=proc,
            writes=writes,
            config=config,
            triggers=triggers,
            id=task_id,
            path=(PULL, name),
            writers=node_info.get("writers", []),
        ))

    return {t.id: t for t in tasks}


def _read_proc_input(
    channels: Mapping[str, BaseChannel],
    node_channels: list[str] | str,
) -> Any:
    if isinstance(node_channels, list):
        val = {}
        for chan in node_channels:
            if chan not in channels:
                return MISSING
            if channels[chan].is_available():
                val[chan] = channels[chan].get()
            else:
                return MISSING
        return val
    elif isinstance(node_channels, str):
        if node_channels in channels and channels[node_channels].is_available():
            return channels[node_channels].get()
        return MISSING
    return MISSING


def local_read(
    channels: Mapping[str, BaseChannel],
    task_writes: list,
    select: list[str] | str,
    fresh: bool = False,
) -> dict[str, Any] | Any:
    """Read current state, optionally applying task writes first."""
    updated: dict[str, list] = defaultdict(list)
    if isinstance(select, str):
        for c, v in task_writes:
            if c == select:
                updated[c].append(v)
    else:
        for c, v in task_writes:
            if c in select:
                updated[c].append(v)

    if fresh and updated:
        local_channels: dict[str, BaseChannel] = {}
        for k in channels:
            if k in updated:
                cc = channels[k].copy()
                cc.update(updated[k])
                local_channels[k] = cc
            else:
                local_channels[k] = channels[k]
        return read_channels(local_channels, select)

    return read_channels(channels, select)
