FROM python:3.10-slim

WORKDIR /app

# Install necessary packages
RUN apt-get update && apt-get install -y \
    wget \
    libnss3 \
    libgconf-2-4 \
    libxss1 \
    libx11-xcb1 \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Install Playwright and its dependencies
RUN pip install --no-cache-dir playwright
RUN playwright install
COPY . .

CMD ["python", "main.py"]
