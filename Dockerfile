FROM python:3.7-slim

WORKDIR /app

# Install system dependencies forexconnect may need
RUN apt-get update && apt-get install -y \
    build-essential \
    libssl-dev \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application
COPY . .

# Set environment variables
ENV PYTHONUNBUFFERED=1

# Expose port
ENV PORT=8080
EXPOSE 8080

# Run the bridge
CMD ["python", "bridge.py"]
