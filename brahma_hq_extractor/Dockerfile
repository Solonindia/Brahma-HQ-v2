FROM python:3.11-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY extractor ./extractor
ENV PORT=8080
CMD ["uvicorn", "extractor.main:app", "--host", "0.0.0.0", "--port", "8080"]
