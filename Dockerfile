# Dockerfile
FROM python:3.12-slim

# System deps for psycopg2
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential gcc libpq-dev && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Install Python deps first (layer caching)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy just the app code we need
COPY src ./src
COPY schema.sql ./schema.sql

# Expose the Prometheus metrics port
EXPOSE 8000

# Run the poller
CMD ["python", "-m", "src.main"]
