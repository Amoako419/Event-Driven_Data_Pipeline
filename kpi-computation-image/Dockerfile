# Base image
FROM python:3.9-slim

# Set environment variables for Spark and Hadoop
ENV SPARK_VERSION=3.3.2
ENV HADOOP_VERSION=3 
ENV PYSPARK_PYTHON=python3
ENV JAVA_HOME=/usr/lib/jvm/default-java 

# --- AWS S3 Dependencies ---
# Choose versions compatible with Hadoop 3.3.x (often used with Spark 3.3.2 hadoop3 profile)
ENV HADOOP_AWS_VERSION=3.3.1 
ENV AWS_SDK_VERSION=1.11.901 

# Install Java, wget, and clean up
RUN apt-get update && \
    apt-get install -y --no-install-recommends default-jdk wget && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Verify Java installation (Optional)
RUN java -version

# Download and install Spark
RUN wget --no-verbose https://archive.apache.org/dist/spark/spark-$SPARK_VERSION/spark-$SPARK_VERSION-bin-hadoop$HADOOP_VERSION.tgz && \
    tar -xvzf spark-$SPARK_VERSION-bin-hadoop$HADOOP_VERSION.tgz && \
    mv spark-$SPARK_VERSION-bin-hadoop$HADOOP_VERSION /opt/spark && \
    rm spark-$SPARK_VERSION-bin-hadoop$HADOOP_VERSION.tgz

# Configure Spark environment variables
ENV SPARK_HOME=/opt/spark
ENV PATH=$SPARK_HOME/bin:$JAVA_HOME/bin:$PATH
ENV PYTHONPATH=$SPARK_HOME/python/:$SPARK_HOME/python/lib/py4j-0.10.9.5-src.zip

# --- Download AWS JARs for S3 access ---
RUN wget https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/${HADOOP_AWS_VERSION}/hadoop-aws-${HADOOP_AWS_VERSION}.jar -P $SPARK_HOME/jars/ && \
    wget https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/${AWS_SDK_VERSION}/aws-java-sdk-bundle-${AWS_SDK_VERSION}.jar -P $SPARK_HOME/jars/

# Set working directory
WORKDIR /app

# Copy and install Python dependencies (requirements.txt should include boto3 now)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the entire application context (including all scripts)
COPY . .


# For Dockerfile.kpi_computation:
CMD ["python3", "task2.py"]