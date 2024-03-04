# Stage 1: Build Stage
FROM python:3.9-slim AS build

# Install any necessary build dependencies here if needed

# Set working directory
WORKDIR /app

# Install dependencies
RUN pip install --no-cache-dir pyspark matplotlib findspark scikit-learn pandas

# Install necessary runtime dependencies
RUN apt-get update && apt-get install -y default-jre-headless && \
    apt-get clean

# Set working directory
WORKDIR /app

# Copy installed dependencies from the build stage
COPY --from=build /usr/local/lib/python3.9/site-packages/ /usr/local/lib/python3.9/site-packages/
COPY . /app

# CMD to run the Python script
CMD ["python", "main.py"]

