#!/usr/bin/env python3

"""Test script to reproduce the AsyncSqliteSaver :memory: isolation issue."""

import asyncio
import sys
sys.path.append('.')

from zerograph import AsyncSqliteSaver, Checkpoint, CheckpointMetadata


async def test_memory_isolation():
    """Test that demonstrates the isolation issue."""
    print("Testing AsyncSqliteSaver with :memory: database...")
    
    # Create AsyncSqliteSaver with :memory: database
    saver = AsyncSqliteSaver(":memory:")
    
    # First checkpoint
    cfg1 = {"configurable": {"thread_id": "thread1"}}
    checkpoint1 = {
        "id": "cp1",
        "channel_values": {"value": 42}
    }
    metadata1 = {"source": "test", "step": 1}
    
    # Put checkpoint
    result1 = await saver.aput(cfg1, checkpoint1, metadata1)
    print(f"Put checkpoint 1: {result1}")
    
    # Try to retrieve it
    retrieved1 = await saver.aget_tuple(result1)
    if retrieved1:
        print(f"Retrieved checkpoint 1 value: {retrieved1.checkpoint.get('channel_values', {}).get('value')}")
    else:
        print("ERROR: Could not retrieve checkpoint 1!")
    
    saver.close()
    print("Test completed.")


if __name__ == "__main__":
    asyncio.run(test_memory_isolation())
