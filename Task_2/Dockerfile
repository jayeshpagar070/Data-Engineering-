# Using official Python image from the Docker Hub
FROM python:3.9-slim

# Copy requirements.txt to the root directory of the container
COPY requirements.txt /

# Installing the dependencies
RUN pip install --no-cache-dir -r /requirements.txt

# Copy the rest of the application code to the container's root
COPY . /

# Set environment variables
ENV PYTHONUNBUFFERED=1

# Run Python script (adjust path if script.py is not in root)
CMD ["python", "/script.py"]
