# Step 1: Use an official Python image as the base image
FROM python:3.9-slim

# Step 2: Set environment variables for Spark installation
ENV SPARK_VERSION=3.3.2
ENV HADOOP_VERSION=3.2
ENV PYSPARK_PYTHON=python3

# Step 3: Install Java (required for Spark)
RUN apt-get update && \
    apt-get install -y openjdk-11-jdk wget && \
    apt-get clean;

# Step 4: Install Spark and Hadoop
RUN wget https://dlcdn.apache.org/spark/spark-$SPARK_VERSION/spark-$SPARK_VERSION-bin-hadoop$HADOOP_VERSION.tgz && \
    tar -xvzf spark-$SPARK_VERSION-bin-hadoop$HADOOP_VERSION.tgz && \
    mv spark-$SPARK_VERSION-bin-hadoop$HADOOP_VERSION /opt/spark && \
    rm spark-$SPARK_VERSION-bin-hadoop$HADOOP_VERSION.tgz

# Step 5: Set environment variables for Spark
ENV SPARK_HOME=/opt/spark
ENV PATH=$SPARK_HOME/bin:$PATH
ENV PYTHONPATH=$SPARK_HOME/python:$PYTHONPATH

# Step 6: Copy the requirements.txt file into the container
COPY requirements.txt /app/requirements.txt

# Step 7: Install dependencies from requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

# Step 8: Set the working directory
WORKDIR /app

# Step 9: Copy your PySpark script into the Docker container
COPY . /app

# Step 10: Command to run the PySpark script (replace with your script name)
CMD ["python3", "img-script.py"]
