# Use an official Python runtime as a parent image
FROM python
# Set the working directory to /app
WORKDIR /app

# Copy the current directory contents into the container at /app
COPY requirements.txt /app
COPY storage_server.py /app


# Install any needed packages specified in requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Make port 5000 available to the world outside this container
EXPOSE 5000 80 2000

# Run .py when the container launches
CMD ["python", "storage_server.py", "-p 5000" ] 
