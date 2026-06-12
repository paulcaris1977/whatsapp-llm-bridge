from fastapi import FastAPI, Depends, HTTPException, Security
from fastapi.security import APIKeyHeader
import httpx
import os
from typing import List, Dict

app = FastAPI(title="WhatsApp LLM Bridge")

API_KEY          = os.getenv("API_KEY")
INTERNAL_API_KEY = os.getenv("INTERNAL_API_KEY")
BRIDGE_URL       = os.getenv("BRIDGE_URL", "http://localhost:8081")

if not API_KEY:
    raise RuntimeError("API_KEY environment variable is not set")
if not INTERNAL_API_KEY:
    raise RuntimeError("INTERNAL_API_KEY environment variable is not set")

api_key_header = APIKeyHeader(name="X-API-Key", auto_error=False)


def verify_api_key(x_api_key: str = Security(api_key_header)) -> str:
    if x_api_key != API_KEY:
        raise HTTPException(status_code=403, detail="Clé API invalide")
    return x_api_key


@app.get("/messages", response_model=List[Dict])
async def get_messages(api_key: str = Depends(verify_api_key)):
    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            resp = await client.get(
                f"{BRIDGE_URL}/messages",
                headers={"X-API-Key": INTERNAL_API_KEY},
            )
            resp.raise_for_status()
            return resp.json()
    except httpx.HTTPStatusError as e:
        raise HTTPException(status_code=502, detail=f"Bridge error: {e.response.status_code}")
    except httpx.RequestError as e:
        raise HTTPException(status_code=503, detail=f"Bridge unreachable: {e}")


@app.get("/health")
async def health():
    return {"status": "running"}
