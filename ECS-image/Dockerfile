FROM python:3.9-slim

ENV SPARK_VERSION=3.3.2
ENV HADOOP_VERSION=3.2
ENV PYSPARK_PYTHON=python3

RUN apt-get update && \
    apt-get install -y openjdk-11-jdk wget && \
    apt-get clean

RUN wget https://dlcdn.apache.org/spark/spark-$SPARK_VERSION/spark-$SPARK_VERSION-bin-hadoop$HADOOP_VERSION.tgz && \
    tar -xvzf spark-$SPARK_VERSION-bin-hadoop$HADOOP_VERSION.tgz && \
    mv spark-$SPARK_VERSION-bin-hadoop$HADOOP_VERSION /opt/spark && \
    rm spark-$SPARK_VERSION-bin-hadoop$HADOOP_VERSION.tgz

ENV SPARK_HOME=/opt/spark
ENV PATH=$SPARK_HOME/bin:$PATH
ENV PYTHONPATH=$SPARK_HOME/python:$PYTHONPATH

# Copy requirements and install them
COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

# Set working directory and copy scripts and data
WORKDIR /app
COPY . /app

# Default CMD (can be overridden by Docker Compose)
CMD ["python3", "task1.py"]
