import asyncio
from uuid import uuid4
from datetime import datetime
import heapq

# in-memory data
status_store = {}
batch_queue = []

lock = asyncio.Lock()

# maps priority to int for sorting
priority_map = {"HIGH": 0, "MEDIUM": 1, "LOW": 2}

async def enqueue_batches(ids, priority, ingestion_id):
    created_time = datetime.utcnow().timestamp()
    batches = [ids[i:i+3] for i in range(0, len(ids), 3)]
    status_store[ingestion_id] = {"batches": []}

    for batch in batches:
        batch_id = str(uuid4())
        batch_info = {
            "batch_id": batch_id,
            "ids": batch,
            "status": "yet_to_start"
        }
        status_store[ingestion_id]["batches"].append(batch_info)

        async with lock:
            heapq.heappush(batch_queue, (
                priority_map[priority],
                created_time,
                ingestion_id,
                batch_id,
                batch
            ))

async def process_batches():
    while True:
        if not batch_queue:
            await asyncio.sleep(1)
            continue

        async with lock:
            priority, created_time, ingestion_id, batch_id, ids = heapq.heappop(batch_queue)

        # Mark status as triggered
        for batch in status_store[ingestion_id]["batches"]:
            if batch["batch_id"] == batch_id:
                batch["status"] = "triggered"

        # Simulate external API
        await asyncio.sleep(1)  # simulate fetch delay
        print(f"Processed batch: {ids}")

        # Update status to completed
        for batch in status_store[ingestion_id]["batches"]:
            if batch["batch_id"] == batch_id:
                batch["status"] = "completed"

        await asyncio.sleep(5)  # rate limit: 1 batch every 5 seconds

# Start background task

