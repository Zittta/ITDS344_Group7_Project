FROM python:3.12-slim

WORKDIR /app

# Install dependencies for all streaming services
# kafka-python-ng is a maintained fork of kafka-python that supports Python 3.12+
RUN pip install --no-cache-dir \
    kafka-python-ng==2.2.3 \
    requests==2.31.0 \
    pandas==2.2.2

# Copy all streaming scripts
COPY kafka_producer.py .
COPY consumer_bronze.py .
