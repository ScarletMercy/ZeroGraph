"""Functional API - @entrypoint and @task decorators for workflow definition."""

from __future__ import annotations

import asyncio
import copy
import logging
import functools
import inspect
import threading
from collections.abc import Callable, Generator, AsyncGenerator
from typing import Any

from zerograph.checkpoint.base import BaseCheckpointSaver
from zerograph.constants import PREVIOUS

logger = logging.getLogger(__name__)

__all__ = ("entrypoint", "task")


class _TaskFuture:
    """Future-like object returned by @task decorated functions."""

    __slots__ = ("_func", "_args", "_kwargs", "_result", "_done", "_exception", "_lock", "_executing", "_event")

    def __init__(self, func: Callable, args: tuple, kwargs: dict) -> None:
        self._func = func
        self._args = args
        self._kwargs = kwargs
        self._result = None
        self._done = False
        self._exception = None
        self._lock = threading.Lock()
        self._executing = False
        self._event = None

    def result(self) -> Any:
        with self._lock:
            if self._executing:
                raise RuntimeError(
                    f"Task '{self._func.__name__}' is already being executed "
                    "asynchronously. Use await task.aresult() instead."
                )
            if not self._done:
                try:
                    raw = self._func(*self._args, **self._kwargs)
                    if asyncio.iscoroutine(raw):
                        raw.close()
                        raise RuntimeError(
                            f"Task '{self._func.__name__}' is an async function. "
                            "Use await task.aresult() instead of task.result()."
                        )
                    self._result = raw
                    self._done = True
                    self._func = None
                    self._args = None
                    self._kwargs = None
                except Exception as e:
                    self._exception = e
                    self._done = True
                    self._func = None
                    self._args = None
                    self._kwargs = None
                    raise
            if self._exception is not None:
                raise self._exception
            return self._result

    async def aresult(self) -> Any:
        # threading.Lock.acquire() is used intentionally: the critical section
        # only reads/writes a few booleans (nanoseconds).  After Bug #4 fix,
        # result() will refuse to run concurrently, so this never blocks long.
        should_execute = False
        self._lock.acquire()
        try:
            if self._done:
                if self._exception is not None:
                    raise self._exception
                return self._result
            if not self._executing:
                self._executing = True
                should_execute = True
        finally:
            self._lock.release()

        if should_execute:
            try:
                if asyncio.iscoroutinefunction(self._func):
                    result = await self._func(*self._args, **self._kwargs)
                else:
                    result = self._func(*self._args, **self._kwargs)
            except Exception as e:
                with self._lock:
                    self._exception = e
                    self._done = True
                    self._executing = False
                    self._func = None
                    self._args = None
                    self._kwargs = None
                if self._event is not None:
                    self._event.set()
                raise
            with self._lock:
                self._result = result
                self._done = True
                self._executing = False
                self._func = None
                self._args = None
                self._kwargs = None
            if self._event is not None:
                self._event.set()
            return result

        with self._lock:
            if self._event is None:
                self._event = asyncio.Event()
            event = self._event
        await event.wait()
        with self._lock:
            if self._exception is not None:
                raise self._exception
            return self._result


def task(func: Callable) -> Callable:
    """Decorator to mark a function as a discrete work unit.

    The decorated function returns a _TaskFuture instead of executing
    immediately. Call .result() to get the value synchronously or
    await .aresult() for async execution.
    """
    @functools.wraps(func)
    def wrapper(*args, **kwargs) -> _TaskFuture:
        return _TaskFuture(func, args, kwargs)
    wrapper._is_task = True
    return wrapper


def _resolve_futures(obj: Any) -> Any:
    """Recursively resolve _TaskFuture objects."""
    if isinstance(obj, _TaskFuture):
        return obj.result()
    elif isinstance(obj, (list, tuple)):
        resolved = [_resolve_futures(item) for item in obj]
        if hasattr(type(obj), '_make'):
            return type(obj)._make(resolved)
        return type(obj)(resolved)
    elif isinstance(obj, dict):
        return {k: _resolve_futures(v) for k, v in obj.items()}
    elif isinstance(obj, (set, frozenset)):
        return type(obj)(_resolve_futures(item) for item in obj)
    return obj


async def _aresolve_futures(obj: Any) -> Any:
    """Recursively resolve _TaskFuture objects (async)."""
    if isinstance(obj, _TaskFuture):
        return await obj.aresult()
    elif isinstance(obj, (list, tuple)):
        resolved = [await _aresolve_futures(item) for item in obj]
        if hasattr(type(obj), '_make'):
            return type(obj)._make(resolved)
        return type(obj)(resolved)
    elif isinstance(obj, dict):
        return {k: await _aresolve_futures(v) for k, v in obj.items()}
    elif isinstance(obj, (set, frozenset)):
        return type(obj)([await _aresolve_futures(item) for item in obj])
    return obj


class _EntrypointWrapper:
    """Wrapper returned by @entrypoint decorator."""

    def __init__(
        self,
        func: Callable,
        checkpointer: BaseCheckpointSaver | None = None,
        store: Any | None = None,
    ) -> None:
        self._func = func
        self._checkpointer = checkpointer
        self._store = store
        functools.update_wrapper(self, func)

    def _get_previous(self, config: dict | None) -> Any:
        """Get the previous run's return value from checkpointer."""
        if self._checkpointer is None or config is None:
            return None
        thread_id = config.get("configurable", {}).get("thread_id", "__default__")
        # Use a special key to store/retrieve the last return value
        prev_config = {
            "configurable": {
                "thread_id": thread_id,
                "checkpoint_ns": PREVIOUS,
            }
        }
        cp = self._checkpointer.get(prev_config)
        if cp and "channel_values" in cp:
            return cp["channel_values"].get("__root__")
        return None

    def _save_previous(self, result: Any, config: dict | None) -> None:
        """Save the return value as previous for next invocation."""
        if self._checkpointer is None or config is None:
            return
        thread_id = config.get("configurable", {}).get("thread_id", "__default__")
        from datetime import datetime, timezone
        import uuid
        prev_config = {
            "configurable": {
                "thread_id": thread_id,
                "checkpoint_ns": PREVIOUS,
            }
        }
        try:
            saved = copy.deepcopy(result)
        except Exception:
            logger.warning(
                "deepcopy failed for entrypoint result; "
                "storing original reference. Mutating the return value "
                "will corrupt the checkpointed previous state.",
                exc_info=True,
            )
            saved = result
        from zerograph.checkpoint.base import Checkpoint, CheckpointMetadata
        cp: Checkpoint = {
            "v": 1,
            "id": str(uuid.uuid4()),
            "ts": datetime.now(timezone.utc).isoformat(),
            "channel_values": {"__root__": saved},
            "channel_versions": {"__root__": 1},
            "versions_seen": {},
        }
        self._checkpointer.put(
            prev_config, cp,
            CheckpointMetadata(source="previous", step=0),
        )

    def _build_kwargs(self, inp: Any, config: dict | None) -> dict[str, Any]:
        """Build keyword arguments for the entrypoint function."""
        sig = inspect.signature(self._func)
        kwargs: dict[str, Any] = {}
        params = list(sig.parameters.items())
        first_param_name = params[0][0] if params else None
        for param_name, param in params:
            if param_name == first_param_name:
                continue
            if param_name in ("previous",):
                kwargs[param_name] = self._get_previous(config)
            elif param_name in ("store",):
                kwargs[param_name] = self._store
            elif param_name in ("config",):
                kwargs[param_name] = config or {}
            elif param_name in ("writer",):
                kwargs[param_name] = lambda x: None  # no-op writer for invoke
        return kwargs

    def invoke(self, input: Any, config: dict | None = None) -> Any:
        """Execute the workflow synchronously."""
        if asyncio.iscoroutinefunction(self._func):
            raise TypeError(
                "Cannot invoke() an async function synchronously. "
                "Use ainvoke() instead."
            )
        kwargs = self._build_kwargs(input, config)
        result = self._func(input, **kwargs)
        result = _resolve_futures(result)
        self._save_previous(result, config)
        return result

    async def ainvoke(self, input: Any, config: dict | None = None) -> Any:
        """Execute the workflow asynchronously."""
        kwargs = self._build_kwargs(input, config)
        if asyncio.iscoroutinefunction(self._func):
            result = await self._func(input, **kwargs)
        else:
            result = self._func(input, **kwargs)
        result = await _aresolve_futures(result)
        self._save_previous(result, config)
        return result

    def stream(self, input: Any, config: dict | None = None, *,
               stream_mode: str = "updates") -> Generator:
        """Execute the workflow and yield stream events.

        Note: because the entrypoint function is a regular function,
        all events are collected during execution and yielded after
        completion. For true streaming, use StateGraph with generator nodes.
        """
        if asyncio.iscoroutinefunction(self._func):
            raise TypeError(
                "Cannot stream() an async function synchronously. "
                "Use astream() instead."
            )
        kwargs = self._build_kwargs(input, config)
        events: list = []

        def writer(value: Any) -> None:
            events.append(value)

        if "writer" in kwargs:
            kwargs["writer"] = writer

        result = self._func(input, **kwargs)
        result = _resolve_futures(result)
        self._save_previous(result, config)

        # Yield custom events first, then final result
        if stream_mode == "custom":
            for ev in events:
                yield ev
        elif stream_mode == "updates":
            if events:
                yield {"entrypoint:events": events}
            yield {"entrypoint": result}

    async def astream(self, input: Any, config: dict | None = None, *,
                      stream_mode: str = "updates") -> AsyncGenerator:
        """Execute the workflow asynchronously and yield stream events."""
        kwargs = self._build_kwargs(input, config)
        events: list = []

        def writer(value: Any) -> None:
            events.append(value)

        if "writer" in kwargs:
            kwargs["writer"] = writer

        if asyncio.iscoroutinefunction(self._func):
            result = await self._func(input, **kwargs)
        else:
            result = self._func(input, **kwargs)
        result = await _aresolve_futures(result)
        self._save_previous(result, config)

        if stream_mode == "custom":
            for ev in events:
                yield ev
        elif stream_mode == "updates":
            if events:
                yield {"entrypoint:events": events}
            yield {"entrypoint": result}


def entrypoint(
    checkpointer: BaseCheckpointSaver | None = None,
    store: Any | None = None,
) -> Callable:
    """Decorator to mark a function as a workflow entry point.

    Usage:
        @entrypoint(checkpointer=InMemorySaver())
        def workflow(inp, *, previous=None, store=None, config=None, writer=None):
            result = my_task(inp["x"])
            return result.result()

        workflow.invoke({"x": 1})
    """
    def decorator(func: Callable) -> _EntrypointWrapper:
        return _EntrypointWrapper(func, checkpointer=checkpointer, store=store)
    return decorator
