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
    content: str

replicated_messages: List[str] = []

@app.post("/replicate")
async def replicate_message(message: Message):
    logger.info(f"Received replication request with message: {message.content}")

    await asyncio.sleep(2)

    replicated_messages.append(message.content)

    logger.info(f"Message replicated: {message.content}")
    return {"status": "Message replicated"}

@app.get("/messages")
async def get_messages():
    logger.info("Received GET request for replicated messages")
    return {"messages": replicated_messages}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8001)
