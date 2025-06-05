import pytest
from httpx import AsyncClient
from main import app
import asyncio

@pytest.mark.asyncio
async def test_priority_and_rate_limit():
    async with AsyncClient(app=app, base_url="http://test") as ac:
        res1 = await ac.post("/ingest", json={"ids": list(range(1, 6)), "priority": "MEDIUM"})
        res2 = await ac.post("/ingest", json={"ids": list(range(6, 10)), "priority": "HIGH"})

        id1 = res1.json()["ingestion_id"]
        id2 = res2.json()["ingestion_id"]

        await asyncio.sleep(30)  # Let batches process

        r1 = await ac.get(f"/status/{id1}")
        r2 = await ac.get(f"/status/{id2}")

        assert r1.status_code == 200
        assert r2.status_code == 200

        # Check correct order of processing
        assert r2.json()["status"] == "completed"
        assert r1.json()["status"] in ["triggered", "completed"]
