# Lightweight Python image
FROM python:3.11-slim

WORKDIR /app

# System deps for pandas/openpyxl if needed
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# Python deps
RUN pip install --no-cache-dir streamlit pandas altair openpyxl

# Copy app in (we also mount it as a volume for hot-reload, but copying gives a sane default)
COPY app_streamlit.py /app/app_streamlit.py

# Streamlit config to make it friendlier in containers
ENV STREAMLIT_BROWSER_GATHER_USAGE_STATS="false" \
    STREAMLIT_SERVER_HEADLESS="true" \
    STREAMLIT_SERVER_PORT="8501" \
    STREAMLIT_SERVER_ADDRESS="0.0.0.0"

EXPOSE 8501

CMD ["streamlit", "run", "app_streamlit.py"]
