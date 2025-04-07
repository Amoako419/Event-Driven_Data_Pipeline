FROM python:3.9-slim

ENV SPARK_VERSION=3.3.2
# Note: Hadoop version 3.2 might need specific configuration or jars
# Spark 3.3.2 is often distributed with Hadoop 3.3 binaries. Consider using:
# ENV HADOOP_BINARY_VERSION=3 # (Spark usually handles this)
# or check the download page: spark-3.3.2-bin-hadoop3.tgz might be more standard
ENV HADOOP_VERSION=3 
ENV PYSPARK_PYTHON=python3

# Try installing default-jdk instead of the specifically versioned package
RUN apt-get update && \
    apt-get install -y --no-install-recommends default-jdk wget && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/* # Clean up apt lists to reduce image size

# Verify Java installation (Optional but recommended)
RUN java -version

# Using spark-3.3.2-bin-hadoop3 which is a common distribution name
RUN wget https://archive.apache.org/dist/spark/spark-$SPARK_VERSION/spark-$SPARK_VERSION-bin-hadoop$HADOOP_VERSION.tgz && \
    tar -xvzf spark-$SPARK_VERSION-bin-hadoop$HADOOP_VERSION.tgz && \
    mv spark-$SPARK_VERSION-bin-hadoop$HADOOP_VERSION /opt/spark && \
    rm spark-$SPARK_VERSION-bin-hadoop$HADOOP_VERSION.tgz

ENV SPARK_HOME=/opt/spark
ENV PATH=$SPARK_HOME/bin:$PATH
# Ensure Python path includes PySpark libraries correctly
ENV PYTHONPATH=$SPARK_HOME/python/:$SPARK_HOME/python/lib/py4j-0.10.9.5-src.zip:$PYTHONPATH

# Copy requirements and install them
COPY requirements.txt /app/requirements.txt
WORKDIR /app 
RUN pip install --no-cache-dir -r requirements.txt

# Set working directory and copy scripts and data
# WORKDIR /app # Already set
COPY . /app

# Default CMD (can be overridden by Docker Compose)
# Ensure your script name matches exactly
CMD ["python3", "task1.py"] 