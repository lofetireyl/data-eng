FROM python:3.11-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY getsongbpm_worker.py .
CMD ["python", "-u", "getsongbpm_worker.py"]