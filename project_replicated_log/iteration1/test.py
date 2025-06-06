import pytest
import httpx
import asyncio
from time import time

MASTER_URL = "http://localhost:8000"
SECONDARY1_URL = "http://localhost:8001"
SECONDARY2_URL = "http://localhost:8002"

@pytest.mark.asyncio
async def test_append_and_replicate():
    async with httpx.AsyncClient() as client:
        start_time = time()
        response = await client.post(f"{MASTER_URL}/messages", json={"content": "Test message"})
        end_time = time()

        assert response.status_code == 200
        assert response.json()["message"] == "Test message"
        assert end_time - start_time >= 4.0

        master_response = await client.get(f"{MASTER_URL}/messages")
        assert master_response.status_code == 200
        assert "Test message" in master_response.json()["messages"]

        for secondary_url in [SECONDARY1_URL, SECONDARY2_URL]:
            secondary_response = await client.get(f"{secondary_url}/messages")
            assert secondary_response.status_code == 200
            assert "Test message" in secondary_response.json()["messages"]

@pytest.mark.asyncio
async def test_get_empty_messages():
    async with httpx.AsyncClient() as client:
        response = await client.get(f"{MASTER_URL}/messages")
        assert response.status_code == 200
        assert isinstance(response.json()["messages"], list)

        for secondary_url in [SECONDARY1_URL, SECONDARY2_URL]:
            response = await client.get(f"{secondary_url}/messages")
            assert response.status_code == 200
            assert isinstance(response.json()["messages"], list)
