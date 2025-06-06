import logging
import asyncio
import os
import uuid
from fastapi import FastAPI, HTTPException
import httpx
from pydantic import BaseModel
from typing import List
from fastapi.responses import JSONResponse

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

messages: List[StoredMessage] = []

SECONDARIES = ["http://secondary1:8001", "http://secondary2:8001"]

@app.post("/messages")
async def append_message(message: Message):
    if message.w < 1 or message.w > len(SECONDARIES) + 1:
        logger.error(f"Invalid write concern: w={message.w}")
        raise HTTPException(status_code=400, detail=f"Write concern must be between 1 and {len(SECONDARIES) + 1}")

    message_id = str(uuid.uuid4())
    if any(m.content == message.content for m in messages):
        logger.info(f"Message with content: {message.content} already exists, returning existing message_id")
        existing_message = next(m for m in messages if m.content == message.content)
        return {"status": "Message already exists", "message_id": existing_message.message_id, "content": message.content}

    logger.info(f"Received POST request with message_id: {message_id}, content: {message.content}, w: {message.w}")

    messages.append(StoredMessage(message_id=message_id, content=message.content))

    async def replicate_to_secondary(secondary: str):
        async with httpx.AsyncClient() as client:
            try:
                response = await client.post(
                    f"{secondary}/replicate",
                    json={"message_id": message_id, "content": message.content},
                    timeout=10.0
                )
                if response.status_code == 200:
                    logger.info(f"Successfully replicated to {secondary}")
                    return True
                else:
                    logger.error(f"Failed to replicate to {secondary}: {response.status_code}")
                    return False
            except httpx.RequestError as e:
                logger.error(f"Error replicating to {secondary}: {str(e)}")
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
    return {"messages": [{"message_id": m.message_id, "content": m.content} for m in messages]}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
