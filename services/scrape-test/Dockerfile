## Use the official Python slim image as the base
FROM python:3.10-slim
 
# Set environment variables
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1
 
# Set the working directory
WORKDIR /app
 
# Install system dependencies required by Playwright
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    libssl-dev \
    libffi-dev \
    libxml2-dev \
    libxslt1-dev \
    libjpeg-dev \
    libglib2.0-0 \
    libsm6 \
    libxext6 \
    libxrender-dev \
    libnss3 \
    libatk1.0-0 \
    libatk-bridge2.0-0 \
    libcups2 \
    libdrm2 \
    libgbm1 \
    libgtk-3-0 \
    libxcomposite1 \
    libxdamage1 \
    libxrandr2 \
    xdg-utils \
    && rm -rf /var/lib/apt/lists/*
 
# Upgrade pip using Python's -m flag
RUN python -m pip install --upgrade pip
 
# Install Playwright and its dependencies
RUN pip install playwright
RUN playwright install --with-deps
 
# Copy requirements.txt and install Python dependencies
COPY requirements.txt .
RUN python -m pip install --no-cache-dir -r requirements.txt
 
# Copy the entire project
COPY . .
 
# Expose port 8080 for Cloud Run
EXPOSE 8080

# Run the application
CMD ["python", "app/main.py"]
