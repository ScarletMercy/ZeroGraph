#!/usr/bin/env python3

import asyncio
import concurrent.futures
import threading
import sqlite3
from zerograph.checkpoint.sqlite import AsyncSqliteSaver

def show_thread_info():
    return f'Thread {threading.current_thread().name} (ID: {threading.current_thread().ident})'

def debug_get_conn(saver):
    """Debug version of _get_conn that prints thread info"""
    print(f'   _get_conn called in: {show_thread_info()}')
    if not hasattr(saver._local, "conn") or saver._local.conn is None:
        print(f'   Creating new connection in: {show_thread_info()}')
        conn = sqlite3.connect(saver._conn_string, check_same_thread=False)
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA foreign_keys=ON")
        conn.execute("PRAGMA busy_timeout=5000")
        conn.row_factory = sqlite3.Row
        saver._local.conn = conn
        saver._setup_conn(conn)
        print(f'   Schema set up in: {show_thread_info()}')
    else:
        print(f'   Reusing existing connection in: {show_thread_info()}')
    return saver._local.conn

# Monkey patch to debug
AsyncSqliteSaver._get_conn = debug_get_conn

async def test_flow():
    print('=== DEBUGGING THE EXACT FLOW ===')
    print(f'Main thread: {show_thread_info()}')
    
    # 1. __init__ in main thread
    print('\n1. __init__ in main thread:')
    saver = AsyncSqliteSaver(':memory:')
    
    # 2. put() in main thread  
    print('\n2. put() in main thread:')
    cfg = {'configurable': {'thread_id': 'main'}}
    checkpoint = {'id': 'test_cp', 'channel_values': {'value': 42}}
    metadata = {'source': 'test'}
    
    # Manually call _get_conn to see what happens
    print('   put() calls _get_conn:')
    conn1 = saver._get_conn()
    
    # Insert data directly to see thread isolation
    conn1.execute(
        "INSERT INTO checkpoints (thread_id, checkpoint_ns, checkpoint_id, checkpoint, metadata, created_at) VALUES (?, ?, ?, ?, ?, ?)",
        ('main', '', 'test_cp', '{"channel_values": {"value": 42}}', '{}', '2026-05-20T00:00:00')
    )
    conn1.commit()
    print(f'   Data inserted in: {show_thread_info()}')
    
    # 3. aget_tuple via executor thread
    print('\n3. aget_tuple via executor thread:')
    print('   aget_tuple calls _to_thread...')
    
    def test_aget_tuple():
        print(f'     Inside _to_thread in: {show_thread_info()}')
        print('     _to_thread calls super().get_tuple...')
        print('     super().get_tuple calls _get_conn:')
        conn2 = saver._get_conn()  # This will create a NEW connection in executor thread
        print(f'       _get_conn returns connection in: {show_thread_info()}')
        
        # Try to query the data
        row = conn2.execute(
            "SELECT checkpoint_id, checkpoint FROM checkpoints WHERE thread_id='main' AND checkpoint_id='test_cp'"
        ).fetchone()
        print(f'     Query result: {row is not None} in: {show_thread_info()}')
        return row
    
    result = await saver._to_thread(test_aget_tuple)
    print(f'   Final result: {result is not None} in: {show_thread_info()}')
    
    saver.close()

asyncio.run(test_flow())
