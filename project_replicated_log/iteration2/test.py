import pytest
import httpx
import asyncio
from time import time

MASTER_URL = "http://localhost:8000"
SECONDARY1_URL = "http://localhost:8001"
SECONDARY2_URL = "http://localhost:8002"

@pytest.mark.asyncio
async def test_write_concern_1():
    async with httpx.AsyncClient() as client:
        start_time = time()
        response = await client.post(f"{MASTER_URL}/messages", json={"content": "Test w=1", "w": 1})
        end_time = time()

        assert response.status_code == 200
        assert response.json()["content"] == "Test w=1"
        assert end_time - start_time < 1.0
        master_response = await client.get(f"{MASTER_URL}/messages")
        assert master_response.status_code == 200
        assert any(m["content"] == "Test w=1" for m in master_response.json()["messages"])
        await asyncio.sleep(3)
        for secondary_url in [SECONDARY1_URL, SECONDARY2_URL]:
            secondary_response = await client.get(f"{secondary_url}/messages")
            assert secondary_response.status_code == 200
            assert any(m["content"] == "Test w=1" for m in secondary_response.json()["messages"])

@pytest.mark.asyncio
async def test_write_concern_2():
    async with httpx.AsyncClient() as client:
        start_time = time()
        response = await client.post(f"{MASTER_URL}/messages", json={"content": "Test w=2", "w": 2})
        end_time = time()

        assert response.status_code == 200
        assert response.json()["content"] == "Test w=2"
        assert 2.0 <= end_time - start_time < 3.0

        master_response = await client.get(f"{MASTER_URL}/messages")
        assert master_response.status_code == 200
        assert any(m["content"] == "Test w=2" for m in master_response.json()["messages"])

        secondary1_response = await client.get(SECONDARY1_URL + "/messages")
        assert secondary1_response.status_code == 200
        assert any(m["content"] == "Test w=2" for m in secondary1_response.json()["messages"])

        secondary2_response = await client.get(SECONDARY2_URL + "/messages")
        assert secondary2_response.status_code == 200
        has_message = any(m["content"] == "Test w=2" for m in secondary2_response.json()["messages"])
        if not has_message:
            await asyncio.sleep(3)
            secondary2_response = await client.get(SECONDARY2_URL + "/messages")
            assert any(m["content"] == "Test w=2" for m in secondary2_response.json()["messages"])

@pytest.mark.asyncio
async def test_write_concern_3():
    async with httpx.AsyncClient() as client:
        start_time = time()
        response = await client.post(f"{MASTER_URL}/messages", json={"content": "Test w=3", "w": 3})
        end_time = time()

        assert response.status_code == 200
        assert response.json()["content"] == "Test w=3"
        assert end_time - start_time >= 4.0

        master_response = await client.get(f"{MASTER_URL}/messages")
        assert master_response.status_code == 200
        assert any(m["content"] == "Test w=3" for m in master_response.json()["messages"])

        for secondary_url in [SECONDARY1_URL, SECONDARY2_URL]:
            secondary_response = await client.get(f"{secondary_url}/messages")
            assert secondary_response.status_code == 200
            assert any(m["content"] == "Test w=3" for m in secondary_response.json()["messages"])

@pytest.mark.asyncio
async def test_deduplication():
    async with httpx.AsyncClient() as client:
        response1 = await client.post(f"{MASTER_URL}/messages", json={"content": "Duplicate test", "w": 3})
        response2 = await client.post(f"{MASTER_URL}/messages", json={"content": "Duplicate test", "w": 3})

        master_response = await client.get(f"{MASTER_URL}/messages")
        assert master_response.status_code == 200
        duplicates = sum(1 for m in master_response.json()["messages"] if m["content"] == "Duplicate test")
        assert duplicates == 1

        await asyncio.sleep(3)
        for secondary_url in [SECONDARY1_URL, SECONDARY2_URL]:
            secondary_response = await client.get(f"{secondary_url}/messages")
            assert secondary_response.status_code == 200
            duplicates = sum(1 for m in secondary_response.json()["messages"] if m["content"] == "Duplicate test")
            assert duplicates == 1

@pytest.mark.asyncio
async def test_total_ordering():
    async with httpx.AsyncClient() as client:
        messages = ["First", "Second", "Third"]
        for msg in messages:
            await client.post(f"{MASTER_URL}/messages", json={"content": msg, "w": 3})
        await asyncio.sleep(3)

        master_response = await client.get(f"{MASTER_URL}/messages")
        assert master_response.status_code == 200
        master_messages = [m["content"] for m in master_response.json()["messages"]][-3:]
        assert master_messages == messages

        for secondary_url in [SECONDARY1_URL, SECONDARY2_URL]:
            secondary_response = await client.get(f"{secondary_url}/messages")
            assert secondary_response.status_code == 200
            secondary_messages = [m["content"] for m in secondary_response.json()["messages"]][-3:]
            assert secondary_messages == messages
