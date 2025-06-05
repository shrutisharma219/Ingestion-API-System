from fastapi import FastAPI
from uuid import uuid4
from models import IngestRequest, Priority
from queue_processor import enqueue_batches, status_store, process_batches
import uvicorn
import asyncio

app = FastAPI()

@app.on_event("startup")
async def startup_event():
    asyncio.create_task(process_batches())

@app.post("/ingest")
async def ingest(data: IngestRequest):
    ingestion_id = str(uuid4())
    await enqueue_batches(data.ids, data.priority, ingestion_id)
    return {"ingestion_id": ingestion_id}

@app.get("/status/{ingestion_id}")
async def status(ingestion_id: str):
    if ingestion_id not in status_store:
        return {"error": "Ingestion ID not found"}

    batches = status_store[ingestion_id]["batches"]
    all_statuses = [batch["status"] for batch in batches]

    if all(s == "yet_to_start" for s in all_statuses):
        overall = "yet_to_start"
    elif all(s == "completed" for s in all_statuses):
        overall = "completed"
    else:
        overall = "triggered"

    return {
        "ingestion_id": ingestion_id,
        "status": overall,
        "batches": batches
    }

if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=5000, reload=True)
