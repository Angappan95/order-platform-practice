import asyncio
import os
from uuid import uuid4
from fastapi import FastAPI, HTTPException

app = FastAPI()

@app.get("/health")
async def health():
    return {
        "dependency": os.getenv("DEPENDENCY_NAME", "mock"),
        "status": "ok"
    }

@app.post("/authorizations")
async def authorize():
    delay = float(os.getenv("DELAY_SECONDS", "0"))
    if delay:
        await asyncio.sleep(delay)
    if os.getenv("FAIL_MODE") == "500":
        raise HTTPException(500, "Simulated payment outage")
    if os.getenv("FAIL_MODE") == "402":
        raise HTTPException(402, "Similated card decline")
    return {"authorization_id": str(uuid4())}

@app.post("/reservations")
async def reserve():
    return {"reservation_id": str(uuid4())}