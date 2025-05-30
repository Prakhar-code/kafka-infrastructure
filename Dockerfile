FROM python:3.12-slim

# Install system dependencies
RUN apt-get update && apt-get install -y \
    build-essential \
    librdkafka1 \
    librdkafka-dev \
    pkg-config \
    python3-dev \
    && rm -rf /var/lib/apt/lists/*

# Set environment variables for the build
ENV CFLAGS="-I/usr/include/librdkafka"
ENV LDFLAGS="-L/usr/lib"

WORKDIR /app

# Copy only requirements first to leverage Docker cache
COPY requirements.txt .

# Upgrade pip and install dependencies
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

# Copy the rest of the application
COPY src/ ./src
COPY config/kafka/.env ./config/kafka/.env

EXPOSE 9092

CMD ["python", "src/connector/lambda_connector.py"]
