FROM python:3.11-slim

# Set working directory
WORKDIR /app

# Copy requirements first (better caching)
COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

# Copy project files
COPY app ./app
COPY ui ./ui

# Cloud Run requires this
ENV PORT=8080

# Start server (IMPORTANT LINE)
CMD exec uvicorn app.main:app --host 0.0.0.0 --port ${PORT}
