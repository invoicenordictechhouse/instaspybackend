# Dockerfile for Google Ads Ingestion

# Use Python base image
FROM python:3.10-slim

# Set the working directory inside the container
WORKDIR /app

# Copy requirements.txt to the container
COPY requirements.txt .

# Install Python dependencies
RUN pip install --uppgrade pip --no-cache-dir -r requirements.txt

# Copy the entire application code
COPY . /app

# Expose port (if needed)
EXPOSE 8080

# Start the FastAPI server
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8080"]
