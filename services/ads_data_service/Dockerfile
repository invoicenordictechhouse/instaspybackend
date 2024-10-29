# Stage 1: Builder Stage
# Use official Python slim image
FROM python:3.10-slim AS builder

# Set the working directory
WORKDIR /app

# Copy requirements.txt
COPY requirements.txt .

# Install dependencies in a temporary builder stage
RUN pip install --no-cache-dir --user -r requirements.txt

# Stage 2: Final Image
# Use a clean Python slim image as the base for the final image
FROM python:3.10-slim

# Set the working directory
WORKDIR /app

# Copy installed dependencies from the builder stage
COPY --from=builder /root/.local /root/.local

# Ensure the PATH includes the local pip packages
ENV PATH=/root/.local/bin:$PATH

# Copy the app code
COPY . .

# Expose the port for FastAPI/Uvicorn
EXPOSE 8080

# Set a non-root user
RUN useradd -m appuser
USER appuser

# Command to start the FastAPI app
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8080"]
