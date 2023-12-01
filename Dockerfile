# Use a base Spark image
FROM bitnami/spark:3.2.0

# Set the working directory
WORKDIR /app

# Copy the requirements file
COPY requirements.txt /app/requirements.txt

# Install Python dependencies
RUN pip install --no-cache-dir -r /app/requirements.txt

# Copy the PySpark script and input files to the container
COPY transformation_scripts/transpose_data.py /app/transformation_scripts/transpose_data.py
COPY input_data/*.csv /app/input_data/

# Run the PySpark script
CMD spark-submit /app/transformation_scripts/transpose_data.py

