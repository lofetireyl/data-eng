FROM python:3.11-slim

ENV PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

# Systemabhängigkeiten (optional für Debug)
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates curl netcat-openbsd && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY requirements.txt .
RUN pip install -r requirements.txt

COPY producer.py .

CMD ["python", "producer.py"]