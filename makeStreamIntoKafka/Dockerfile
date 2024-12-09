# Use an official Python runtime as a parent image
FROM python:3.9-slim

# Set the working directory in the container
WORKDIR /app

# Copy the current directory contents into the container at /app
COPY app.py /app
COPY data.txt /app
# Install required Python packages
RUN pip install kafka-python

# Make port 9092 available to the world outside this container (optional, depending on your Kafka setup)
EXPOSE 9092

# Run app.py when the container launches
CMD ["python", "app.py"]
