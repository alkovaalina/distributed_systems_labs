import logging
import asyncio
import os
from fastapi import FastAPI, HTTPException
import httpx
import random
from pydantic import BaseModel
from typing import List, Optional

os.makedirs('/app/logs', exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('/app/logs/secondary.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

app = FastAPI()

MASTER_URL = "http://master:8000"

class Message(BaseModel):
    message_id: str
    content: str
    order: int

class SyncRequest(BaseModel):
    last_message_id: Optional[str]

replicated_messages: List[Message] = []
max_display_order = 0

async def sync_with_master():
    async with httpx.AsyncClient(timeout=10.0) as client:
        try:
            last_message_id = replicated_messages[-1].message_id if replicated_messages else None
            response = await client.post(
                f"{MASTER_URL}/sync",
                json={"last_message_id": last_message_id}
            )
            if response.status_code == 200:
                new_messages = response.json()["messages"]
                for msg in sorted(new_messages, key=lambda x: x["order"]):
                    if not any(m.message_id == msg["message_id"] for m in replicated_messages):
                        replicated_messages.append(Message(**msg))
                        logger.info(f"Synced message: {msg['message_id']}, {msg['content']}, order={msg['order']}")
                global max_display_order
                max_display_order = max(m.order for m in replicated_messages) if replicated_messages else 0
                logger.info(f"Sync completed, max_display_order={max_display_order}")
                logger.info(f"Replicated messages after sync: {[(m.message_id, m.content, m.order) for m in replicated_messages]}")
            else:
                logger.error(f"Sync failed: {response.status_code}")
        except httpx.RequestError as e:
            logger.error(f"Error syncing with master: {str(e)}")

@app.on_event("startup")
async def startup_event():
    logger.info("Starting up, syncing with master")
    await sync_with_master()

@app.post("/clear")
async def clear_messages():
    replicated_messages.clear()
    global max_display_order
    max_display_order = 0
    logger.info("Messages cleared")
    return {"status": "Messages cleared"}

@app.post("/replicate")
async def replicate_message(message: Message):
    logger.info(f"Received replication request with message_id: {message.message_id}, content: {message.content}, order: {message.order}")
    if any(m.message_id == message.message_id for m in replicated_messages):
        logger.info(f"Message with message_id: {message.message_id} already exists, skipping")
        return {"status": "Message already exists"}

    if random.random() < 0.2:
        logger.warning(f"Generating random 500 error for message_id: {message.message_id}")
        raise HTTPException(status_code=500, detail="Random internal server error")

    await asyncio.sleep(2)
    replicated_messages.append(message)
    global max_display_order
    max_display_order = max(m.order for m in replicated_messages)
    logger.info(f"Message replicated: {message.message_id}, {message.content}, order={message.order}")
    return {"status": "Message replicated"}

@app.get("/messages")
async def get_messages():
    logger.info("Received GET request for replicated messages")
    logger.info(f"All replicated messages: {[(m.message_id, m.content, m.order) for m in replicated_messages]}")
    display_messages = []
    expected_order = 1
    for msg in sorted(replicated_messages, key=lambda m: m.order):
        if msg.order == expected_order:
            display_messages.append(msg)
            expected_order += 1
        else:
            logger.warning(f"Order mismatch: expected {expected_order}, got {msg.order}")
            break
    logger.info(f"Displayed messages: {[(m.message_id, m.content, m.order) for m in display_messages]}")
    return {"messages": [{"message_id": m.message_id, "content": m.content, "order": m.order} for m in display_messages]}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8001)
