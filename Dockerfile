FROM python:3.12-slim

LABEL maintainer="trevorwilf"
LABEL description="Tax Collector — Unified trade data aggregator for tax reporting"

# System deps
RUN apt-get update && \
    apt-get install -y --no-install-recommends curl && \
    rm -rf /var/lib/apt/lists/*

# App directory
WORKDIR /app

# Install Python deps
COPY app/requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY app/ /app/

# Export directory
RUN mkdir -p /app/exports

# Healthcheck — verify FastAPI is responding
HEALTHCHECK --interval=30s --timeout=5s --retries=3 --start-period=15s \
    CMD python -c "import socket; s=socket.create_connection(('127.0.0.1',8100),2); s.close()" || exit 1

EXPOSE 8100

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8100", "--log-level", "info"]
