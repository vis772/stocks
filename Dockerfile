FROM python:3.12-slim

# Avoid interactive prompts during package installation
ENV DEBIAN_FRONTEND=noninteractive

# System dependencies:
#   libpq-dev      — psycopg2-binary build headers (runtime linking)
#   gcc / g++      — compile any C extensions (scipy, numpy wheels may need them)
#   libgomp1       — OpenMP runtime needed by some numerical libs
#   curl           — useful for health-checks / debugging
#   ca-certificates — HTTPS requests from inside the container
RUN apt-get update && apt-get install -y --no-install-recommends \
        libpq-dev \
        gcc \
        g++ \
        libgomp1 \
        curl \
        ca-certificates \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Install Python dependencies first (layer-cache friendly)
COPY requirements.txt .
RUN pip install --no-cache-dir --upgrade pip \
    && pip install --no-cache-dir -r requirements.txt

# Copy application source
COPY . .

# Ensure the shared data directory exists inside the image
RUN mkdir -p /app/data

# No CMD here — docker-compose.yml sets the command per service
