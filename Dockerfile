# Use Python base image
FROM python:3.11-slim

# Set working directory in the container
WORKDIR /app

# Install OS dependencies (optional but good for bigquery + SSL issues)
RUN apt-get update && apt-get install -y gcc libpq-dev

# Copy Python dependencies
COPY requirements.txt .

# Install Python packages
RUN pip install --no-cache-dir -r requirements.txt

# Copy rest of the codebase
COPY . .

# Expose port (if you switch to FastAPI + uvicorn later)
EXPOSE 8000

# Run the app
CMD ["python", "main.py"]
