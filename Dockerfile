# Use an official Python runtime as a parent image
FROM python:3.12.1-slim

# Set the working directory in the container
WORKDIR /app

# Copy the current directory contents into the container at /app
COPY . /app

# Install any needed dependencies specified in requirements.txt
# (You can skip this step if your script doesn't have any dependencies)
RUN pip install --no-cache-dir -r requirements.txt

# Run script.py when the container launches
CMD ["python", "index.py"]