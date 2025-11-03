FROM python:3.11-slim
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1
WORKDIR /app

# (optional) useful base tools
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates tzdata \
 && rm -rf /var/lib/apt/lists/*

# install deps first for better layer caching
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# copy your producer code
COPY kafka_producer_new_releases.py /app/producer.py

# COPY wait-for-kafka.sh /app/wait-for-kafka.sh
# RUN chmod +x /app/wait-for-kafka.sh

# if you use the wait-for-kafka entrypoint
# COPY entrypoint.sh /app/entrypoint.sh
# RUN chmod +x /app/entrypoint.sh
# CMD ["/app/entrypoint.sh"]

CMD ["python", "/app/producer.py"]
