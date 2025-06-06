import logging
import asyncio
import os
from fastapi import FastAPI
from pydantic import BaseModel
from typing import List

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

class Message(BaseModel):
    message_id: str
    content: str

replicated_messages: List[Message] = []

@app.post("/replicate")
async def replicate_message(message: Message):
    logger.info(f"Received replication request with message_id: {message.message_id}, content: {message.content}")

    if any(m.message_id == message.message_id for m in replicated_messages):
        logger.info(f"Message with message_id: {message.message_id} already exists, skipping")
        return {"status": "Message already replicated"}

    await asyncio.sleep(2)
    replicated_messages.append(message)

    logger.info(f"Message replicated: {message.message_id}, {message.content}")
    return {"status": "Message replicated"}

@app.get("/messages")
async def get_messages():
    logger.info("Received GET request for replicated messages")
    return {"messages": [{"message_id": m.message_id, "content": m.content} for m in replicated_messages]}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8001)
