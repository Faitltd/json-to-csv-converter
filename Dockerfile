FROM python:3.9-slim

WORKDIR /app

COPY . .

RUN pip install --no-cache-dir -r requirements.txt

# Create directories for uploads and outputs
RUN mkdir -p uploads outputs

# Make sure the directories are writable
RUN chmod 777 uploads outputs

# Set environment variables
ENV PORT=8080

# Run the web service on container startup
CMD exec gunicorn --bind :$PORT --workers 1 --threads 8 --timeout 0 app:app
