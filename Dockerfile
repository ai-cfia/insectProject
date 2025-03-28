# Use Debian-based Python image for better compatibility
FROM python:3.13-slim

# Set environment variables
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

# Set working directory
WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    git \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements first to leverage Docker cache
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy models directory first
COPY models/ ./models/

# Copy the rest of the application
COPY . .

# Create necessary directories
RUN mkdir -p cache

# Set the entrypoint to python run.py
ENTRYPOINT ["python", "run.py"]

# Set default command (can be overridden at runtime)
CMD ["observations"]
