FROM golang:1.25-alpine AS go-builder
RUN apk add --no-cache git ca-certificates
WORKDIR /app
COPY whatsapp-bridge/go.mod whatsapp-bridge/go.sum ./
RUN go mod download
COPY whatsapp-bridge/ .
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" -o whatsapp-bridge main.go

FROM python:3.12-slim
WORKDIR /app
RUN apt-get update && apt-get install -y --no-install-recommends \
    ffmpeg ca-certificates supervisor && rm -rf /var/lib/apt/lists/*
COPY server/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY --from=go-builder /app/whatsapp-bridge /app/whatsapp-bridge
COPY server/ .
COPY supervisord.conf /etc/supervisor/conf.d/supervisord.conf
RUN useradd -m -u 1000 appuser && \
    mkdir -p store /var/log/supervisor && \
    chown -R appuser:appuser /app /var/log/supervisor
USER appuser
EXPOSE 8000
CMD ["supervisord", "-c", "/etc/supervisor/conf.d/supervisord.conf"]
