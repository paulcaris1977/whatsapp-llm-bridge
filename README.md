# whatsapp-llm-bridge

Go 1.25 + FastAPI bridge between WhatsApp and LLM agents.

## Stack

- `whatsmeow` (Go) on port 8081
- FastAPI proxy on port 8000
- SQLite via `modernc.org/sqlite` (`CGO_ENABLED=0`)
- Docker + supervisord
- `whatsapp_mcp.py` (MCP server for Claude Desktop)

## Run

```bash
docker compose up -d --build
```

## Configuration

`.env`:
```
API_KEY=your_external_key
INTERNAL_API_KEY=internal_key
BRIDGE_URL=http://localhost:8081
```

## API

Auth: `X-API-Key` header (value = `API_KEY`)

- `GET /messages?limit=50&after_date=...&before_date=...&contact=...`
- `GET /contacts`
- `POST /send` → `{ "to": "+33612345678", "message": "text" }`
- `GET /health`

## MCP Tools

- `get_whatsapp_messages`
- `get_whatsapp_contacts`
- `get_whatsapp_history`
- `send_whatsapp_message`

Copy `whatsapp_mcp.py` into Claude Desktop MCP config.
