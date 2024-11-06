# Dockerfile

FROM mcr.microsoft.com/playwright/python:v1.36.0-focal

# Set the working directory in the container
WORKDIR /app

# Copy only the requirements file to leverage Docker cache
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy the rest of the application code into the container
COPY . .

# Expose the port that the app runs on
EXPOSE 8000

# Run the application
CMD ["python", "app/main.py"]
