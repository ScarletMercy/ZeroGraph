"""Tests for SQLite checkpoint backend."""

import json
import os
import tempfile
import threading

import pytest

from zerograph.checkpoint.sqlite import SqliteSaver
from zerograph.checkpoint.base import Checkpoint, CheckpointMetadata, PendingWrite


def _make_config(thread_id="t1", checkpoint_ns="", checkpoint_id=None):
    config = {"configurable": {"thread_id": thread_id, "checkpoint_ns": checkpoint_ns}}
    if checkpoint_id:
        config["configurable"]["checkpoint_id"] = checkpoint_id
    return config


def _make_checkpoint(**overrides):
    import uuid
    from datetime import datetime, timezone
    cp = {
        "v": 1,
        "id": str(uuid.uuid4()),
        "ts": datetime.now(timezone.utc).isoformat(),
        "channel_values": {"x": 0},
        "channel_versions": {"x": 1},
        "versions_seen": {},
    }
    cp.update(overrides)
    return cp


@pytest.fixture
def saver():
    with SqliteSaver(":memory:") as s:
        yield s


class TestSqliteSaver:

    def test_put_and_get(self, saver):
        cp = _make_checkpoint(channel_values={"x": 42})
        meta = CheckpointMetadata(source="input", step=0)
        config = _make_config("t1")

        result_config = saver.put(config, cp, meta)
        assert "configurable" in result_config
        assert "checkpoint_id" in result_config["configurable"]

        tup = saver.get_tuple(config)
        assert tup is not None
        assert tup.checkpoint["channel_values"]["x"] == 42
        assert tup.metadata["source"] == "input"
        assert tup.parent_config is None

    def test_get_nonexistent(self, saver):
        result = saver.get_tuple(_make_config("nonexistent"))
        assert result is None

    def test_put_with_parent(self, saver):
        cp1 = _make_checkpoint(channel_values={"x": 1})
        meta1 = CheckpointMetadata(source="input", step=0)
        config1 = saver.put(_make_config("t1"), cp1, meta1)

        cp2 = _make_checkpoint(channel_values={"x": 2})
        meta2 = CheckpointMetadata(source="loop", step=1)
        # Use first checkpoint_id as parent
        parent_id = config1["configurable"]["checkpoint_id"]
        config2 = saver.put(
            _make_config("t1", checkpoint_id=None),
            cp2,
            meta2,
        )

        tup = saver.get_tuple(_make_config("t1"))
        assert tup is not None
        assert tup.checkpoint["channel_values"]["x"] == 2

    def test_list(self, saver):
        for i in range(5):
            cp = _make_checkpoint(channel_values={"x": i})
            meta = CheckpointMetadata(source="loop", step=i)
            saver.put(_make_config("t1"), cp, meta)

        results = saver.list(_make_config("t1"), limit=3)
        assert len(results) == 3
        # Should be ordered by created_at DESC
        steps = [r.metadata["step"] for r in results]
        assert steps == sorted(steps, reverse=True)

    def test_list_with_before(self, saver):
        configs = []
        for i in range(5):
            cp = _make_checkpoint(channel_values={"x": i})
            meta = CheckpointMetadata(source="loop", step=i)
            c = saver.put(_make_config("t1"), cp, meta)
            configs.append(c)

        before = configs[3]
        results = saver.list(_make_config("t1"), limit=10, before=before)
        assert len(results) == 3  # Only checkpoints before the 4th

    def test_pending_writes(self, saver):
        cp = _make_checkpoint()
        meta = CheckpointMetadata(source="input", step=0)
        config = saver.put(_make_config("t1"), cp, meta)
        cp_id = config["configurable"]["checkpoint_id"]

        writes = [
            ("task1", "channel_a", "value_a"),
            ("task1", "channel_b", 42),
        ]
        saver.put_writes(config, writes, "task1")

        loaded = saver.get_pending_writes(config)
        assert len(loaded) == 2
        assert loaded[0] == ("task1", "channel_a", "value_a")
        assert loaded[1] == ("task1", "channel_b", 42)

    def test_pending_writes_replace(self, saver):
        cp = _make_checkpoint()
        meta = CheckpointMetadata(source="input", step=0)
        config = saver.put(_make_config("t1"), cp, meta)

        saver.put_writes(config, [("task1", "ch", "old")], "task1")
        saver.put_writes(config, [("task1", "ch", "new")], "task1")

        loaded = saver.get_pending_writes(config)
        assert len(loaded) == 1
        assert loaded[0][2] == "new"

    def test_delete_thread(self, saver):
        cp = _make_checkpoint()
        meta = CheckpointMetadata(source="input", step=0)
        saver.put(_make_config("t1"), cp, meta)
        saver.put(_make_config("t2"), _make_checkpoint(), meta)

        saver.delete_thread("t1")
        assert saver.get_tuple(_make_config("t1")) is None
        assert saver.get_tuple(_make_config("t2")) is not None

    def test_json_serialization(self, saver):
        complex_val = {"nested": {"list": [1, 2, 3], "bool": True, "null": None}}
        cp = _make_checkpoint(channel_values=complex_val)
        meta = CheckpointMetadata(source="input", step=0)
        saver.put(_make_config("t1"), cp, meta)

        tup = saver.get_tuple(_make_config("t1"))
        assert tup.checkpoint["channel_values"]["nested"]["list"] == [1, 2, 3]

    def test_wal_mode(self):
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
            db_path = f.name
        try:
            with SqliteSaver(db_path) as s:
                conn = s._get_conn()
                row = conn.execute("PRAGMA journal_mode").fetchone()
                assert row[0].lower() == "wal"
        finally:
            os.unlink(db_path)

    def test_context_manager(self):
        with SqliteSaver(":memory:") as s:
            cp = _make_checkpoint()
            s.put(_make_config("t1"), cp, CheckpointMetadata(source="input", step=0))
            assert s.get_tuple(_make_config("t1")) is not None

    def test_multiple_threads(self):
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
            db_path = f.name

        try:
            # Open one saver to create tables
            with SqliteSaver(db_path) as s:
                pass

            results = {}
            errors = []

            def write_checkpoint(thread_id):
                try:
                    with SqliteSaver(db_path) as s:
                        cp = _make_checkpoint(channel_values={"x": thread_id})
                        meta = CheckpointMetadata(source="input", step=0)
                        s.put(_make_config(thread_id), cp, meta)
                        tup = s.get_tuple(_make_config(thread_id))
                        results[thread_id] = tup.checkpoint["channel_values"]["x"]
                except Exception as e:
                    errors.append(e)

            threads = [threading.Thread(target=write_checkpoint, args=(f"t{i}",)) for i in range(5)]
            for t in threads:
                t.start()
            for t in threads:
                t.join()

            assert not errors
            assert len(results) == 5
            for i in range(5):
                assert results[f"t{i}"] == f"t{i}"
        finally:
            import gc
            gc.collect()
            try:
                os.unlink(db_path)
            except PermissionError:
                pass

    def test_file_persistence(self):
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
            db_path = f.name

        try:
            cp_id = None
            with SqliteSaver(db_path) as s:
                cp = _make_checkpoint(channel_values={"x": 99})
                config = s.put(
                    _make_config("t1"), cp,
                    CheckpointMetadata(source="input", step=0),
                )
                cp_id = config["configurable"]["checkpoint_id"]

            # Reopen and verify data persists
            with SqliteSaver(db_path) as s:
                tup = s.get_tuple(_make_config("t1", checkpoint_id=cp_id))
                assert tup is not None
                assert tup.checkpoint["channel_values"]["x"] == 99
        finally:
            os.unlink(db_path)

    def test_checkpoint_in_graph(self, saver):
        from typing import TypedDict
        from zerograph import StateGraph, START, END

        class St(TypedDict):
            x: int

        def node_a(state):
            return {"x": state["x"] + 1}

        g = StateGraph(St)
        g.add_node("a", node_a)
        g.add_edge(START, "a")
        g.add_edge("a", END)
        compiled = g.compile(checkpointer=saver)

        result = compiled.invoke({"x": 1}, {"configurable": {"thread_id": "g1"}})
        assert result["x"] == 2

        # List should have checkpoints
        cps = saver.list({"configurable": {"thread_id": "g1", "checkpoint_ns": ""}})
        assert len(cps) >= 1
