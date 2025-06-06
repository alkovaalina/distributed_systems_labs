import pytest
import httpx
import asyncio
from time import time
import subprocess

MASTER_URL = "http://localhost:8000"
SECONDARY1_URL = "http://localhost:8001"
SECONDARY2_URL = "http://localhost:8002"

@pytest.mark.asyncio
async def test_write_concern_1():
    async with httpx.AsyncClient(timeout=20.0) as client:
        await clear_all(client)
        start_time = time()
        response = await client.post(f"{MASTER_URL}/messages", json={"content": "Test w=1", "w": 1})
        end_time = time()

        assert response.status_code == 200
        assert response.json()["content"] == "Test w=1"
        assert end_time - start_time < 1.0

        master_response = await client.get(f"{MASTER_URL}/messages")
        assert any(m["content"] == "Test w=1" for m in master_response.json()["messages"])

        await asyncio.sleep(3)
        for secondary_url in [SECONDARY1_URL, SECONDARY2_URL]:
            secondary_response = await client.get(f"{secondary_url}/messages")
            assert secondary_response.status_code == 200
            assert any(m["content"] == "Test w=1" for m in secondary_response.json()["messages"])

@pytest.mark.asyncio
async def test_write_concern_2():
    async with httpx.AsyncClient(timeout=20.0) as client:
        await clear_all(client)
        start_time = time()
        response = await client.post(f"{MASTER_URL}/messages", json={"content": "Test w=2", "w": 2})
        end_time = time()

        assert response.status_code == 200
        assert response.json()["content"] == "Test w=2"
        assert 2.0 <= end_time - start_time < 4.0  # Збільшено межу

        master_response = await client.get(f"{MASTER_URL}/messages")
        assert any(m["content"] == "Test w=2" for m in master_response.json()["messages"])

        secondary1_response = await client.get(f"{SECONDARY1_URL}/messages")
        assert any(m["content"] == "Test w=2" for m in secondary1_response.json()["messages"])

        await asyncio.sleep(3)
        secondary2_response = await client.get(f"{SECONDARY2_URL}/messages")
        has_message = any(m["content"] == "Test w=2" for m in secondary2_response.json()["messages"])
        if not has_message:
            await asyncio.sleep(3)
            secondary2_response = await client.get(f"{SECONDARY2_URL}/messages")
            assert any(m["content"] == "Test w=2" for m in secondary2_response.json()["messages"])

@pytest.mark.asyncio
async def test_write_concern_3():
    async with httpx.AsyncClient(timeout=20.0) as client:
        await clear_all(client)
        start_time = time()
        response = await client.post(f"{MASTER_URL}/messages", json={"content": "Test w=3", "w": 3})
        end_time = time()

        assert response.status_code == 200
        assert response.json()["content"] == "Test w=3"
        assert end_time - start_time >= 4.0

        master_response = await client.get(f"{MASTER_URL}/messages")
        assert any(m["content"] == "Test w=3" for m in master_response.json()["messages"])

        await asyncio.sleep(3)
        for secondary_url in [SECONDARY1_URL, SECONDARY2_URL]:
            secondary_response = await client.get(f"{secondary_url}/messages")
            assert any(m["content"] == "Test w=3" for m in secondary_response.json()["messages"])

@pytest.mark.asyncio
async def test_deduplication():
    async with httpx.AsyncClient(timeout=20.0) as client:
        await clear_all(client)
        response1 = await client.post(f"{MASTER_URL}/messages", json={"content": "Duplicate test", "w": 3})
        response2 = await client.post(f"{MASTER_URL}/messages", json={"content": "Duplicate test", "w": 3})

        master_response = await client.get(f"{MASTER_URL}/messages")
        duplicates = sum(1 for m in master_response.json()["messages"] if m["content"] == "Duplicate test")
        assert duplicates == 1

        await asyncio.sleep(3)
        for secondary_url in [SECONDARY1_URL, SECONDARY2_URL]:
            secondary_response = await client.get(f"{secondary_url}/messages")
            duplicates = sum(1 for m in secondary_response.json()["messages"] if m["content"] == "Duplicate test")
            assert duplicates == 1

@pytest.mark.asyncio
async def test_total_ordering():
    async with httpx.AsyncClient(timeout=20.0) as client:
        await clear_all(client)
        messages = ["First", "Second", "Third"]
        for msg in messages:
            await client.post(f"{MASTER_URL}/messages", json={"content": msg, "w": 3})

        await asyncio.sleep(3)
        master_response = await client.get(f"{MASTER_URL}/messages")
        master_messages = [m["content"] for m in master_response.json()["messages"]][-3:]
        assert master_messages == messages

        for secondary_url in [SECONDARY1_URL, SECONDARY2_URL]:
            secondary_response = await client.get(f"{secondary_url}/messages")
            secondary_messages = [m["content"] for m in secondary_response.json()["messages"]][-3:]
            assert secondary_messages == messages

@pytest.mark.asyncio
async def test_acceptance():
    async with httpx.AsyncClient(timeout=20.0) as client:
        await clear_all(client)

        subprocess.run(["docker-compose", "stop", "secondary2"], check=True)

        response1 = await client.post(f"{MASTER_URL}/messages", json={"content": "Msg1", "w": 1})
        assert response1.status_code == 200
        assert response1.json()["content"] == "Msg1"

        response2 = await client.post(f"{MASTER_URL}/messages", json={"content": "Msg2", "w": 2})
        assert response2.status_code == 200
        assert response2.json()["content"] == "Msg2"

        async def send_msg3():
            start_time = time()
            response = await client.post(f"{MASTER_URL}/messages", json={"content": "Msg3", "w": 3})
            assert response.status_code == 200
            assert response.json()["content"] == "Msg3"
            assert time() - start_time > 2.0
            return response.json()["message_id"]
        msg3_task = asyncio.create_task(send_msg3())

        await asyncio.sleep(0.1)
        response4 = await client.post(f"{MASTER_URL}/messages", json={"content": "Msg4", "w": 1})
        assert response4.status_code == 200
        assert response4.json()["content"] == "Msg4"

        subprocess.run(["docker-compose", "start", "secondary2"], check=True)

        msg3_message_id = await msg3_task

        await asyncio.sleep(10)

        secondary2_response = await client.get(f"{SECONDARY2_URL}/messages")
        secondary2_messages = [m["content"] for m in secondary2_response.json()["messages"]]
        assert secondary2_messages == ["Msg1", "Msg2", "Msg3", "Msg4"]

        secondary2_message_ids = [m["message_id"] for m in secondary2_response.json()["messages"]]
        assert len(secondary2_message_ids) == len(set(secondary2_message_ids))  # Унікальні message_id
        assert msg3_message_id in secondary2_message_ids

async def clear_all(client):
    try:
        await client.post(f"{MASTER_URL}/clear")
        await client.post(f"{SECONDARY1_URL}/clear")
        await client.post(f"{SECONDARY2_URL}/clear")
    except httpx.RequestError:
        pass
