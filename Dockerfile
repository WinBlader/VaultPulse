FROM python:3.10-slim

WORKDIR /app

# Install system utilities and curl for healthchecks
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# Install python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy project files
COPY . .

# Set environment variables for Docker
ENV PYTHONUNBUFFERED=1 \
    HOST=0.0.0.0 \
    PORT=8000 \
    KAFKA_BOOTSTRAP_SERVERS=kafka:29092 \
    HDFS_NAMENODE_URL=hdfs://namenode:9000

# Expose FastAPI web port
EXPOSE 8000

# Launch VaultPulse AI unified pipeline
CMD ["python", "run_project.py"]
