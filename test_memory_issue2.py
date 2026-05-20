#!/usr/bin/env python3

import asyncio
import sys
sys.path.append('.')

from zerograph import AsyncSqliteSaver, Checkpoint, CheckpointMetadata


async def test_multiple_operations():
    """Test multiple operations to ensure schema is created."""
    print("Testing multiple AsyncSqliteSaver operations...")
    
    saver = AsyncSqliteSaver(":memory:")
    
    # Operation 1: Put and get
    cfg1 = {"configurable": {"thread_id": "thread1"}}
    checkpoint1 = {"id": "cp1", "channel_values": {"value": 42}}
    metadata1 = {"source": "test", "step": 1}
    
    result1 = await saver.aput(cfg1, checkpoint1, metadata1)
    retrieved1 = await saver.aget_tuple(result1)
    print(f"Op 1 - Put and get: {retrieved1.checkpoint['channel_values']['value'] if retrieved1 else 'FAILED'}")
    
    # Operation 2: List checkpoints
    list_result = await saver.alist(cfg1, limit=5)
    print(f"Op 2 - List checkpoints found: {len(list_result)}")
    
    # Operation 3: Put writes
    writes = [("task1", "channel1", "value1")]
    await saver.aput_writes(cfg1, writes, "task1")
    
    # Operation 4: Get pending writes
    pending = await saver.aget_pending_writes(result1)
    print(f"Op 4 - Pending writes: {len(pending)}")
    
    saver.close()
    print("All operations completed successfully!")


async def test_concurrent_operations():
    """Test concurrent operations to see if they share the same database."""
    print("\nTesting concurrent operations...")
    
    saver = AsyncSqliteSaver(":memory:")
    
    async def op1():
        cfg = {"configurable": {"thread_id": "thread1"}}
        checkpoint = {"id": "cp1", "channel_values": {"value": 1}}
        metadata = {"source": "test", "step": 1}
        result = await saver.aput(cfg, checkpoint, metadata)
        retrieved = await saver.aget_tuple(result)
        return retrieved.checkpoint["channel_values"]["value"] if retrieved else None
    
    async def op2():
        cfg = {"configurable": {"thread_id": "thread2"}}
        checkpoint = {"id": "cp2", "channel_values": {"value": 2}}
        metadata = {"source": "test", "step": 2}
        result = await saver.aput(cfg, checkpoint, metadata)
        retrieved = await saver.aget_tuple(result)
        return retrieved.checkpoint["channel_values"]["value"] if retrieved else None
    
    results = await asyncio.gather(op1(), op2())
    print(f"Concurrent results: {results}")
    saver.close()


if __name__ == "__main__":
    asyncio.run(test_multiple_operations())
    asyncio.run(test_concurrent_operations())
