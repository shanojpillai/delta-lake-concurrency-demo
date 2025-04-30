FROM python:3.10-slim

# Set environment variables
ENV SPARK_HOME=/opt/spark
ENV PYTHONPATH=$SPARK_HOME/python:$PYTHONPATH
ENV PATH=$SPARK_HOME/bin:$PATH

# Install dependencies
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    default-jdk \
    wget \
    curl \
    procps \
    && rm -rf /var/lib/apt/lists/*

# Install Python dependencies
RUN pip install --no-cache-dir \
    delta-spark==2.4.0 \
    pyspark==3.4.0 \
    pandas \
    numpy \
    matplotlib \
    seaborn \
    kaggle \
    jupyter \
    notebook \
    flask \
    werkzeug \
    papermill

# Download and install Apache Spark
RUN mkdir -p /opt/spark && \
    cd /tmp && \
    wget https://archive.apache.org/dist/spark/spark-3.4.0/spark-3.4.0-bin-hadoop3.tgz && \
    tar -xzf spark-3.4.0-bin-hadoop3.tgz && \
    cp -r spark-3.4.0-bin-hadoop3/* /opt/spark/ && \
    rm -rf spark-3.4.0-bin-hadoop3.tgz spark-3.4.0-bin-hadoop3 && \
    chmod +x /opt/spark/bin/* && \
    # Create symbolic links for Spark binaries
    ln -s /opt/spark/bin/spark-submit /usr/local/bin/spark-submit && \
    ln -s /opt/spark/bin/spark-shell /usr/local/bin/spark-shell && \
    ln -s /opt/spark/bin/pyspark /usr/local/bin/pyspark

# Create directories
RUN mkdir -p /opt/spark/data/raw \
    /opt/spark/data/processed \
    /opt/spark/data/streaming \
    /opt/spark/data/stream_simulator \
    /opt/spark/data/batch_updates \
    /opt/spark/data/checkpoints \
    /opt/spark/notebooks \
    /opt/spark/scripts \
    /opt/spark/app \
    /opt/spark/app/templates \
    /opt/spark/app/static/css \
    /opt/spark/app/static/js \
    /opt/spark/logs

# Download Delta Lake JARs
RUN cd /opt/spark/jars && \
    wget https://repo1.maven.org/maven2/io/delta/delta-core_2.12/2.4.0/delta-core_2.12-2.4.0.jar && \
    wget https://repo1.maven.org/maven2/io/delta/delta-storage/2.4.0/delta-storage-2.4.0.jar

# Set working directory
WORKDIR /opt/spark

# Copy application files
COPY app /opt/spark/app
COPY scripts /opt/spark/scripts
COPY notebooks /opt/spark/notebooks

# Copy entrypoint script
COPY docker/spark/entrypoint.sh /opt/spark/entrypoint.sh
RUN chmod +x /opt/spark/entrypoint.sh

# Expose ports for Spark UI, Jupyter, and Flask app
EXPOSE 4040 8888 5000

# Set entrypoint
ENTRYPOINT ["/opt/spark/entrypoint.sh"]
