from fastapi import FastAPI, Depends, HTTPException, Security
from fastapi.security import APIKeyHeader
import httpx
import os
from typing import List, Dict, Optional
from pydantic import BaseModel

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


# ─── Models ──────────────────────────────────────────────────────────────────

class SendRequest(BaseModel):
    to: str
    message: str


# ─── Routes ──────────────────────────────────────────────────────────────────

@app.get("/messages", response_model=List[Dict])
async def get_messages(
    limit: int = 50,
    after_date: Optional[str] = None,
    before_date: Optional[str] = None,
    contact: Optional[str] = None,
    api_key: str = Depends(verify_api_key)
):
    params = {"limit": limit}
    if after_date:
        params["after_date"] = after_date
    if before_date:
        params["before_date"] = before_date
    if contact:
        params["contact"] = contact
    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            resp = await client.get(
                f"{BRIDGE_URL}/messages",
                headers={"X-API-Key": INTERNAL_API_KEY},
                params=params,
            )
            resp.raise_for_status()
            return resp.json()
    except httpx.HTTPStatusError as e:
        raise HTTPException(status_code=502, detail=f"Bridge error: {e.response.status_code}")
    except httpx.RequestError as e:
        raise HTTPException(status_code=503, detail=f"Bridge unreachable: {e}")


@app.get("/contacts")
async def get_contacts(api_key: str = Depends(verify_api_key)):
    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            resp = await client.get(
                f"{BRIDGE_URL}/contacts",
                headers={"X-API-Key": INTERNAL_API_KEY},
            )
            resp.raise_for_status()
            return resp.json()
    except httpx.HTTPStatusError as e:
        raise HTTPException(status_code=502, detail=f"Bridge error: {e.response.status_code}")
    except httpx.RequestError as e:
        raise HTTPException(status_code=503, detail=f"Bridge unreachable: {e}")


@app.post("/send")
async def send_message(
    body: SendRequest,
    api_key: str = Depends(verify_api_key)
):
    try:
        async with httpx.AsyncClient(timeout=15.0) as client:
            resp = await client.post(
                f"{BRIDGE_URL}/send",
                headers={"X-Internal-Key": INTERNAL_API_KEY},
                json={"to": body.to, "message": body.message},
            )
            resp.raise_for_status()
            return resp.json()
    except httpx.HTTPStatusError as e:
        raise HTTPException(status_code=e.response.status_code, detail=f"Bridge error: {e.response.text}")
    except httpx.RequestError as e:
        raise HTTPException(status_code=503, detail=f"Bridge unreachable: {e}")


@app.get("/health")
async def health():
    return {"status": "running"}
