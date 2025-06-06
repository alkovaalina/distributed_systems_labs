import logging
import asyncio
import os
import uuid
from fastapi import FastAPI, HTTPException
import httpx
from pydantic import BaseModel
from typing import List, Optional

os.makedirs('/app/logs', exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('/app/logs/master.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

app = FastAPI()

class Message(BaseModel):
    content: str
    w: int

class StoredMessage(BaseModel):
    message_id: str
    content: str
    order: int

class SyncRequest(BaseModel):
    last_message_id: Optional[str]

messages: List[StoredMessage] = []
message_order = 0
SECONDARIES = ["http://secondary1:8001", "http://secondary2:8001"]
RETRY_TIMEOUT = 30
HTTP_TIMEOUT = 10.0
order_lock = asyncio.Lock()

@app.post("/clear")
async def clear_messages():
    global messages, message_order
    async with order_lock:
        messages.clear()
        message_order = 0
    logger.info("Messages cleared")
    logger.info(f"Messages after clear: {messages}, message_order: {message_order}")
    return {"status": "Messages cleared"}

@app.post("/messages")
async def append_message(message: Message):
    global message_order
    if message.w < 1 or message.w > len(SECONDARIES) + 1:
        logger.error(f"Invalid write concern: w={message.w}")
        raise HTTPException(status_code=400, detail=f"Write concern must be between 1 and {len(SECONDARIES) + 1}")

    message_id = str(uuid.uuid4())
    if any(m.content == message.content for m in messages):
        logger.info(f"Message with content: {message.content} already exists")
        existing_message = next(m for m in messages if m.content == message.content)
        return {"status": "Message already exists", "message_id": existing_message.message_id, "content": message.content}

    logger.info(f"Received POST request with message_id: {message_id}, content: {message.content}, w: {message.w}")

    async with order_lock:
        message_order += 1
        messages.append(StoredMessage(message_id=message_id, content=message.content, order=message_order))

    async def replicate_to_secondary(secondary: str, attempt: int = 1) -> bool:
        backoff = min(2 ** (attempt - 1), 8)
        start_time = asyncio.get_event_loop().time()
        while asyncio.get_event_loop().time() - start_time < RETRY_TIMEOUT:
            async with httpx.AsyncClient(timeout=HTTP_TIMEOUT) as client:
                try:
                    response = await client.post(
                        f"{secondary}/replicate",
                        json={"message_id": message_id, "content": message.content, "order": message_order}
                    )
                    if response.status_code == 200:
                        logger.info(f"Successfully replicated to {secondary}")
                        return True
                    elif response.status_code == 500:
                        logger.warning(f"Internal server error from {secondary}, retrying after {backoff}s")
                    else:
                        logger.error(f"Failed to replicate to {secondary}: {response.status_code}")
                except httpx.RequestError as e:
                    logger.error(f"Error replicating to {secondary}: {str(e)}")
            await asyncio.sleep(backoff)
            attempt += 1
        logger.error(f"Retry timeout for {secondary} after {RETRY_TIMEOUT}s")
        return False

    successful_acks = 1
    for i in range(min(message.w - 1, len(SECONDARIES))):
        if await replicate_to_secondary(SECONDARIES[i]):
            successful_acks += 1
        else:
            logger.error(f"Failed to get ACK from {SECONDARIES[i]}")
            raise HTTPException(status_code=500, detail=f"Failed to replicate to {SECONDARIES[i]}")

    if successful_acks < message.w:
        logger.error(f"Not enough ACKs: got {successful_acks}, required {message.w}")
        raise HTTPException(status_code=500, detail="Not enough ACKs from secondaries")

    if message.w - 1 < len(SECONDARIES):
        for secondary in SECONDARIES[message.w - 1:]:
            asyncio.create_task(replicate_to_secondary(secondary))

    return {"status": "Message appended and replicated", "message_id": message_id, "content": message.content}

@app.get("/messages")
async def get_messages():
    logger.info("Received GET request for messages")
    return {"messages": [{"message_id": m.message_id, "content": m.content, "order": m.order} for m in messages]}

@app.post("/sync")
async def sync_messages(sync_request: SyncRequest):
    logger.info(f"Received sync request with last_message_id: {sync_request.last_message_id}")
    if sync_request.last_message_id is None:
        return {"messages": [{"message_id": m.message_id, "content": m.content, "order": m.order} for m in messages]}

    try:
        last_message = next(m for m in messages if m.message_id == sync_request.last_message_id)
        return {
            "messages": [
                {"message_id": m.message_id, "content": m.content, "order": m.order}
                for m in messages if m.order > last_message.order
            ]
        }
    except StopIteration:
        logger.error(f"Invalid last_message_id: {sync_request.last_message_id}")
        raise HTTPException(status_code=400, detail="Invalid last_message_id")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
