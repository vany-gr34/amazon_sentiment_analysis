# training-pipeline/Dockerfile

FROM python:3.11-slim

WORKDIR /app

# System deps
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Install Python deps
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy source
COPY src/      ./src/
COPY configs/  ./configs/
COPY data/     ./data/

ENV PYTHONPATH=/app/src
ENV CONFIG_PATH=/app/configs/training.yaml

# Default: run full pipeline (used for one-shot container runs)
# Override CMD in docker-compose to run a specific step
CMD ["python", "src/pipeline.py"]