import logging
import asyncio
import os
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

messages: List[str] = []

SECONDARIES = ["http://secondary1:8001", "http://secondary2:8001"]

@app.post("/messages")
async def append_message(message: Message):
    logger.info(f"Received POST request with message: {message.content}")

    messages.append(message.content)

    async with httpx.AsyncClient() as client:
        for secondary in SECONDARIES:
            try:
                response = await client.post(
                    f"{secondary}/replicate",
                    json={"content": message.content},
                    timeout=10.0
                )
                if response.status_code != 200:
                    logger.error(f"Failed to replicate to {secondary}: {response.status_code}")
                    raise HTTPException(status_code=500, detail=f"Replication failed on {secondary}")
                logger.info(f"Successfully replicated to {secondary}")
            except httpx.RequestError as e:
                logger.error(f"Error replicating to {secondary}: {str(e)}")
                raise HTTPException(status_code=500, detail=f"Replication error on {secondary}")

    return {"status": "Message appended and replicated", "message": message.content}

@app.get("/messages")
async def get_messages():
    logger.info("Received GET request for messages")
    return {"messages": messages}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
