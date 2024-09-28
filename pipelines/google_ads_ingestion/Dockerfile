# Step 1: Build stage (dependency installation)
FROM python:3.10-slim AS builder

# Set the working directory inside the container
WORKDIR /app

# Install build dependencies
RUN apt-get update && apt-get install -y build-essential

# Copy requirements.txt and install Python dependencies
COPY requirements.txt ./
RUN pip install --no-cache-dir --user -r requirements.txt

# Step 2: Final stage (smaller, optimized image)
FROM python:3.10-alpine

# Set the working directory inside the container
WORKDIR /app

# Copy installed dependencies from the build stage
COPY --from=builder /root/.local /root/.local

# Ensure that the Python binaries in the local user directory are used
ENV PATH=/root/.local/bin:$PATH

# Copy the rest of the application code into the container
COPY . .

# Expose port 8080 (for FastAPI)
EXPOSE 8080

# Set a non-root user for better security
RUN addgroup -S appgroup && adduser -S appuser -G appgroup
USER appuser

# Start the FastAPI server using Uvicorn
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8080"]
