# Stage 1: Build stage
FROM openjdk:8-jre-slim AS builder

# Install Python and pip
RUN apt-get update && \
    apt-get install -y python3 python3-pip && \
    apt-get clean

# Set working directory
WORKDIR /app


# Install PySpark, Matplotlib, and findspark
RUN pip3 install pyspark matplotlib findspark

# Copy all Python files from the host into the container
COPY . /app

# Stage 2: Final image
FROM openjdk:8-jre-slim

# Set JAVA_HOME
ENV JAVA_HOME=/usr/local/openjdk-8

# Copy installed dependencies from the builder stage
COPY --from=builder /usr/local/lib/python3.8/site-packages /usr/local/lib/python3.8/site-packages

# Set working directory
WORKDIR /app

# Copy application files
COPY . /app


# CMD to run the Python script
CMD ["python", "-i", "main.py"]

