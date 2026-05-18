"""s08_advanced — 高级功能：error_handler/retry/timeout/cache/store/context/defaults/ABC/errors。"""

from typing import TypedDict

from zerograph import (
    START,
    END,
    StateGraph,
    RetryPolicy,
    TimeoutPolicy,
    InMemoryCache,
    CachePolicy,
    InMemoryStore,
    InMemorySaver,
    BaseCheckpointSaver,
    BaseCache,
    BaseStore,
    EmptyChannelError,
    InvalidUpdateError,
)


class St(TypedDict):
    x: int


# ---------------------------------------------------------------------------
# run()
# ---------------------------------------------------------------------------

def run() -> list[tuple[str, bool, str]]:
    results: list[tuple[str, bool, str]] = []

    # 1. error_handler -------------------------------------------------
    try:
        def bad_fn(state: St) -> dict:
            raise RuntimeError("boom")

        def handler_fn(state: St) -> dict:
            return {"x": -1}

        g = StateGraph(St)
        g.add_node("worker", bad_fn, error_handler="handler")
        g.add_node("handler", handler_fn)
        g.add_edge(START, "worker")
        g.add_edge("worker", "handler")
        g.add_edge("handler", END)
        compiled = g.compile()
        result = compiled.invoke({"x": 0})
        ok = result["x"] == -1
        results.append((
            "error_handler: worker raises, handler catches",
            ok,
            f"result={result}",
        ))
    except Exception as exc:
        results.append(("error_handler: worker raises, handler catches", False, str(exc)))

    # 2. RetryPolicy ---------------------------------------------------
    try:
        counter = {"n": 0}

        def flaky(state: St) -> dict:
            counter["n"] += 1
            if counter["n"] < 3:
                raise ValueError("transient")
            return {"x": state["x"] + 10}

        g = StateGraph(St)
        g.add_node(
            "flaky",
            flaky,
            retry_policy=RetryPolicy(
                max_attempts=3,
                initial_interval=0.01,
                backoff_factor=1.0,
                jitter=False,
                retry_on=ValueError,
            ),
        )
        g.add_edge(START, "flaky")
        g.add_edge("flaky", END)
        result = g.compile().invoke({"x": 1})
        ok = result["x"] == 11 and counter["n"] == 3
        results.append((
            "RetryPolicy: node fails 2 times then succeeds",
            ok,
            f"result={result}, attempts={counter['n']}",
        ))
    except Exception as exc:
        results.append(("RetryPolicy: node fails 2 times then succeeds", False, str(exc)))

    # 3. TimeoutPolicy -------------------------------------------------
    try:
        import asyncio

        # Sub-test A: node runs within timeout
        g = StateGraph(St)
        g.add_node("fast", lambda s: {"x": s["x"] + 1}, timeout=1.0)
        g.add_edge(START, "fast")
        g.add_edge("fast", END)
        compiled = g.compile()
        result = compiled.invoke({"x": 5})
        ok_fast = result["x"] == 6
        stored_timeout = compiled.builder.nodes["fast"].timeout
        ok_timeout_stored = stored_timeout.run_timeout == 1.0

        # Sub-test B: async node exceeds timeout
        async def slow_node(state):
            await asyncio.sleep(5.0)
            return {"x": state["x"] + 1}

        g2 = StateGraph(St)
        g2.add_node("slow", slow_node, timeout=0.05)
        g2.add_edge(START, "slow")
        g2.add_edge("slow", END)
        compiled2 = g2.compile()
        timed_out = False
        try:
            asyncio.run(compiled2.ainvoke({"x": 0}))
        except (TimeoutError, asyncio.TimeoutError):
            timed_out = True

        ok = ok_fast and ok_timeout_stored and timed_out
        results.append((
            "TimeoutPolicy: stored, runs normally, slow node times out",
            ok,
            f"fast_ok={ok_fast}, stored={ok_timeout_stored}, timeout_fired={timed_out}",
        ))
    except Exception as exc:
        results.append(("TimeoutPolicy: timeout stored and node runs normally", False, str(exc)))

    # 4. CachePolicy + InMemoryCache -----------------------------------
    try:
        call_count = {"n": 0}

        def expensive(state: St) -> dict:
            call_count["n"] += 1
            return {"x": state["x"] * 2}

        cache = InMemoryCache()
        g = StateGraph(St)
        g.add_node("cached", expensive, cache_policy=CachePolicy(ttl=60))
        g.add_edge(START, "cached")
        g.add_edge("cached", END)
        compiled = g.compile(cache=cache)

        r1 = compiled.invoke({"x": 5})
        r2 = compiled.invoke({"x": 5})
        ok = r1 == {"x": 10} and r2 == {"x": 10} and call_count["n"] == 1
        results.append((
            "CachePolicy+InMemoryCache: second invoke hits cache",
            ok,
            f"r1={r1}, r2={r2}, call_count={call_count['n']}",
        ))
    except Exception as exc:
        results.append(("CachePolicy+InMemoryCache: second invoke hits cache", False, str(exc)))

    # 5. InMemoryStore + StoreItem -------------------------------------
    try:
        store = InMemoryStore()
        store.put("ns", "k1", "v1")

        def reader(state: St, config) -> dict:
            s = config["configurable"]["__store__"]
            item = s.get("ns", "k1")
            return {"x": state["x"] + (1 if item and item.value == "v1" else 0)}

        g = StateGraph(St)
        g.add_node("reader", reader)
        g.add_edge(START, "reader")
        g.add_edge("reader", END)
        compiled = g.compile(store=store)
        result = compiled.invoke({"x": 0})
        ok = result["x"] == 1
        results.append((
            "InMemoryStore: node reads store via config",
            ok,
            f"result={result}",
        ))
    except Exception as exc:
        results.append(("InMemoryStore: node reads store via config", False, str(exc)))

    # 6. context_schema ------------------------------------------------
    try:

        class Ctx(TypedDict):
            api_key: str

        def ctx_reader(state: St, config) -> dict:
            ctx = config["configurable"]["__context__"]
            return {"x": state["x"] + (1 if ctx.get("api_key") == "secret" else 0)}

        g = StateGraph(St, context_schema=Ctx)
        g.add_node("reader", ctx_reader)
        g.add_edge(START, "reader")
        g.add_edge("reader", END)
        compiled = g.compile(context={"api_key": "secret"})
        result = compiled.invoke({"x": 0})
        ok = result["x"] == 1
        results.append((
            "context_schema: node reads context value",
            ok,
            f"result={result}",
        ))
    except Exception as exc:
        results.append(("context_schema: node reads context value", False, str(exc)))

    # 7. set_node_defaults ---------------------------------------------
    try:
        counter = {"n": 0}

        def flaky_default(state: St) -> dict:
            counter["n"] += 1
            if counter["n"] < 2:
                raise ValueError("oops")
            return {"x": state["x"] + 5}

        g = StateGraph(St)
        g.add_node("worker", flaky_default)
        g.add_edge(START, "worker")
        g.add_edge("worker", END)
        # Apply default retry_policy to all nodes
        g.set_node_defaults(
            retry_policy=RetryPolicy(
                max_attempts=3,
                initial_interval=0.01,
                backoff_factor=1.0,
                jitter=False,
                retry_on=ValueError,
            )
        )
        result = g.compile().invoke({"x": 1})
        ok = result["x"] == 6 and counter["n"] == 2
        results.append((
            "set_node_defaults: default retry_policy applied",
            ok,
            f"result={result}, attempts={counter['n']}",
        ))
    except Exception as exc:
        results.append(("set_node_defaults: default retry_policy applied", False, str(exc)))

    # 8. BaseCheckpointSaver ABC ----------------------------------------
    try:
        import inspect
        # Verify abstract methods exist and InMemorySaver implements them
        has_get = hasattr(BaseCheckpointSaver, "get_tuple")
        has_put = hasattr(BaseCheckpointSaver, "put")
        # InMemorySaver should be a concrete subclass
        is_concrete = not inspect.isabstract(InMemorySaver)
        # Verify it actually works (not just hasattr)
        saver = InMemorySaver()
        test_cfg = {"configurable": {"thread_id": "abc_test"}}
        put_cfg = saver.put(test_cfg, {"id": "abc", "v": 1, "ts": "2025-01-01T00:00:00", "channel_values": {"x": 1}}, {"source": "test"})
        tup = saver.get_tuple(put_cfg)
        ok = has_get and has_put and is_concrete and tup is not None and tup.checkpoint.get("channel_values", {}).get("x") == 1
        results.append((
            "BaseCheckpointSaver ABC + InMemorySaver concrete",
            ok,
            f"abstract_methods={has_get and has_put}, concrete={is_concrete}, round_trip={tup is not None}",
        ))
    except Exception as exc:
        results.append(("BaseCheckpointSaver ABC", False, str(exc)))

    # 9. BaseCache / BaseStore ABCs ------------------------------------
    try:
        import inspect
        has_cache_methods = hasattr(BaseCache, "get") and hasattr(BaseCache, "set")
        has_store_methods = hasattr(BaseStore, "get") and hasattr(BaseStore, "put")
        # Verify concrete implementations work
        cache = InMemoryCache()
        cache.set("k", {"data": 42})
        cache_hit = cache.get("k") == {"data": 42}

        store = InMemoryStore()
        store.put("ns", "k1", "val1")
        item = store.get("ns", "k1")
        store_hit = item is not None and item.value == "val1"

        ok = has_cache_methods and has_store_methods and cache_hit and store_hit
        results.append(("BaseCache/BaseStore ABCs + concrete impls", ok,
                        f"cache_hit={cache_hit}, store_hit={store_hit}"))
    except Exception as exc:
        results.append(("BaseCache/BaseStore ABCs", False, str(exc)))

    # 10. EmptyChannelError / InvalidUpdateError -----------------------
    try:
        from zerograph.channels.last_value import LastValue

        lv = LastValue(int, "err_test")
        # EmptyChannelError
        raised_empty = False
        try:
            lv.get()
        except EmptyChannelError:
            raised_empty = True

        # InvalidUpdateError — two values in one step
        lv.update([1])  # set initial value
        raised_invalid = False
        try:
            lv.update([3, 4])
        except InvalidUpdateError:
            raised_invalid = True

        ok = raised_empty and raised_invalid
        results.append((
            "EmptyChannelError/InvalidUpdateError",
            ok,
            f"empty={raised_empty}, invalid={raised_invalid}",
        ))
    except Exception as exc:
        results.append(("EmptyChannelError/InvalidUpdateError", False, str(exc)))

    return results
