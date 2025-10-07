# build/spark/Dockerfile
FROM apache/spark:3.5.1

# The official image is Debian-based and does NOT have `install_packages`
USER root
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl ca-certificates python3-pip && \
    update-ca-certificates && \
    rm -rf /var/lib/apt/lists/*

# Optional: make sure "python" resolves to python3 (some images do, some don't)
RUN ln -sf /usr/bin/python3 /usr/bin/python

WORKDIR /opt/spark-app

# Python deps used by jobs (psycopg2-binary for MERGE helper)
COPY requirements.txt ./requirements.txt
RUN pip3 install --no-cache-dir -r ./requirements.txt

# Jobs + submit script
COPY jobs/ ./jobs/
COPY submit.sh ./submit.sh
RUN chmod +x ./submit.sh

# Default command: orchestrates bronze/silver/db loader
CMD ["/opt/spark-app/submit.sh"]
