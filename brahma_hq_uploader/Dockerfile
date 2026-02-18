FROM python:3.11-slim

WORKDIR /app

# Copy requirements first
COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

# Copy entire uploader project
COPY . .

# Cloud Run provides PORT
ENV PORT=8080

# Start FastAPI
CMD exec uvicorn app.main:app --host 0.0.0.0 --port ${PORT}
