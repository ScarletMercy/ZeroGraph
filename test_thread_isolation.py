#!/usr/bin/env python3

import asyncio
import concurrent.futures
import sys
sys.path.append('.')

from zerograph.checkpoint.sqlite import AsyncSqliteSaver


def test_synchronous_access():
    """Test accessing the saver synchronously from main thread."""
    print("Testing synchronous access from main thread...")
    
    saver = AsyncSqliteSaver(":memory:")
    
    # This should work - creates tables in main thread
    cfg = {"configurable": {"thread_id": "main"}}
    checkpoint = {"id": "main_cp", "channel_values": {"value": 100}}
    metadata = {"source": "test", "step": 1}
    
    # Call synchronous method directly
    result = saver.put(cfg, checkpoint, metadata)
    print(f"Sync put result: {result}")
    
    # Get it back
    retrieved = saver.get_tuple(result)
    print(f"Sync get result: {retrieved.checkpoint['channel_values']['value'] if retrieved else 'FAILED'}")
    
    saver.close()


async def test_async_access():
    """Test accessing the saver asynchronously via executor thread."""
    print("\nTesting async access via executor thread...")
    
    saver = AsyncSqliteSaver(":memory:")
    
    # This should create tables in executor thread
    cfg = {"configurable": {"thread_id": "async"}}
    checkpoint = {"id": "async_cp", "channel_values": {"value": 200}}
    metadata = {"source": "test", "step": 2}
    
    # Call async method which goes through executor
    result = await saver.aput(cfg, checkpoint, metadata)
    print(f"Async put result: {result}")
    
    # Get it back via async
    retrieved = await saver.aget_tuple(result)
    print(f"Async get result: {retrieved.checkpoint['channel_values']['value'] if retrieved else 'FAILED'}")
    
    saver.close()


async def test_mixed_access():
    """Test mixing synchronous and asynchronous access."""
    print("\nTesting mixed sync/async access...")
    
    saver = AsyncSqliteSaver(":memory:")
    
    # Put via sync
    cfg = {"configurable": {"thread_id": "mixed"}}
    checkpoint = {"id": "mixed_cp", "channel_values": {"value": 300}}
    metadata = {"source": "test", "step": 3}
    
    sync_result = saver.put(cfg, checkpoint, metadata)
    print(f"Sync put result: {sync_result}")
    
    # Get via async
    async_result = await saver.aget_tuple(sync_result)
    print(f"Async get from sync put: {async_result.checkpoint['channel_values']['value'] if async_result else 'FAILED'}")
    
    # Put via async
    async_checkpoint = {"id": "mixed_async_cp", "channel_values": {"value": 400}}
    async_metadata = {"source": "test", "step": 4}
    async_put_result = await saver.aput(cfg, async_checkpoint, async_metadata)
    print(f"Async put result: {async_put_result}")
    
    # Get via sync
    sync_get_result = saver.get_tuple(async_put_result)
    print(f"Sync get from async put: {sync_get_result.checkpoint['channel_values']['value'] if sync_get_result else 'FAILED'}")
    
    saver.close()


if __name__ == "__main__":
    test_synchronous_access()
    asyncio.run(test_async_access())
    asyncio.run(test_mixed_access())
