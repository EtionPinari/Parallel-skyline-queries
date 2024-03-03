FROM openjdk:8-jre-slim

# Install Python and pip
RUN apt-get update && \
    apt-get install -y python3 python3-pip && \
    apt-get clean

# Install PySpark, Matplotlib, and findspark
RUN pip3 install pyspark matplotlib findspark sklearn

# Set working directory
WORKDIR /app

# Copy all Python files from the host into the container
COPY . /app

FROM python:3

# CMD to run the Python script
CMD ["python", "main.py"]

