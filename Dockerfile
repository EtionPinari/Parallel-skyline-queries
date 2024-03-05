# Stage 1: Build Stage
FROM python:3.9-slim AS build

# Set working directory
WORKDIR /app

# Install dependencies
RUN pip install --no-cache-dir pyspark matplotlib findspark scikit-learn pandas

# Install necessary runtime dependencies
RUN apt-get update && apt-get install -y default-jre-headless && \
    apt-get clean

# Set working directory
WORKDIR /app

COPY . /app

# CMD to run the Python script
CMD ["python", "main.py"]

