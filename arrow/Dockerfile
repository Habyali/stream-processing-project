FROM python:3.9-slim

WORKDIR /app

# Install Arrow Flight dependencies
RUN pip install pyarrow pandas

# Create data directory
RUN mkdir -p /data/arrow

# Copy server code
COPY server.py .

# Expose Arrow Flight port
EXPOSE 8815

# Start the Flight server
CMD ["python", "-u", "server.py"]