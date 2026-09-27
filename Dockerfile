FROM ubuntu:24.04

# Avoid interactive installation prompts
ENV DEBIAN_FRONTEND=noninteractive

# Install dependencies, Java 17, SSH server, and Python utilities
RUN apt-get update && apt-get install -y \
    openjdk-17-jdk-headless \
    openssh-server \
    wget \
    curl \
    tar \
    sudo \
    python3 \
    python3-pip \
    python3-venv \
    nano \
    && rm -rf /var/lib/apt/lists/*

# Set environment variables
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV HADOOP_HOME=/opt/hadoop
ENV SPARK_HOME=/opt/spark
ENV PATH=$PATH:$HADOOP_HOME/bin:$HADOOP_HOME/sbin:$SPARK_HOME/bin:$SPARK_HOME/sbin

# Allow Hadoop and YARN daemons to run as root in container
ENV HDFS_NAMENODE_USER="root"
ENV HDFS_DATANODE_USER="root"
ENV HDFS_SECONDARYNAMENODE_USER="root"
ENV YARN_RESOURCEMANAGER_USER="root"
ENV YARN_NODEMANAGER_USER="root"

# Setup passwordless SSH for Hadoop local cluster communication
RUN ssh-keygen -A && \
    ssh-keygen -t rsa -P '' -f ~/.ssh/id_rsa && \
    cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys && \
    chmod 0600 ~/.ssh/authorized_keys

# Install Apache Hadoop 3.5.0 from local downloads archive
ADD downloads/hadoop-3.5.0.tar.gz /opt/
RUN mv /opt/hadoop-3.5.0 /opt/hadoop

# Install Apache Spark 4.2.0 from local downloads archive
ADD downloads/spark-4.2.0-bin-hadoop3.tgz /opt/
RUN mv /opt/spark-4.2.0-bin-hadoop3 /opt/spark

# Set up Python virtual environment with JupyterLab, PySpark bindings, and data science packages
RUN python3 -m venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"
ENV PYSPARK_PYTHON="/opt/venv/bin/python"
ENV PYSPARK_DRIVER_PYTHON="/opt/venv/bin/python"
ENV PYTHONPATH="/workspace:${SPARK_HOME}/python:${PYTHONPATH}"
RUN pip install --no-cache-dir jupyterlab pandas numpy findspark matplotlib

# WebPDF export (Chromium-based, no LaTeX needed)
RUN pip install --no-cache-dir "nbconvert[webpdf]" && \
    playwright install --with-deps chromium

# Configure Hadoop environment and JVM memory limits
RUN echo "export JAVA_HOME=${JAVA_HOME}" >> /opt/hadoop/etc/hadoop/hadoop-env.sh && \
    echo "export HADOOP_HEAPSIZE_MAX=512m" >> /opt/hadoop/etc/hadoop/hadoop-env.sh && \
    echo "export HDFS_NAMENODE_USER=root" >> /opt/hadoop/etc/hadoop/hadoop-env.sh && \
    echo "export HDFS_DATANODE_USER=root" >> /opt/hadoop/etc/hadoop/hadoop-env.sh && \
    echo "export HDFS_SECONDARYNAMENODE_USER=root" >> /opt/hadoop/etc/hadoop/hadoop-env.sh && \
    echo "export YARN_RESOURCEMANAGER_USER=root" >> /opt/hadoop/etc/hadoop/yarn-env.sh && \
    echo "export YARN_NODEMANAGER_USER=root" >> /opt/hadoop/etc/hadoop/yarn-env.sh

# Configure HDFS (core-site.xml & hdfs-site.xml)
RUN echo '<configuration><property><name>fs.defaultFS</name><value>hdfs://localhost:9000</value></property></configuration>' > /opt/hadoop/etc/hadoop/core-site.xml && \
    echo '<configuration><property><name>dfs.replication</name><value>1</value></property><property><name>dfs.datanode.data.dir</name><value>file:///opt/hadoop_tmp/hdfs/datanode</value></property><property><name>dfs.namenode.name.dir</name><value>file:///opt/hadoop_tmp/hdfs/namenode</value></property></configuration>' > /opt/hadoop/etc/hadoop/hdfs-site.xml

# Configure YARN (mapred-site.xml & yarn-site.xml)
RUN echo '<configuration><property><name>mapreduce.framework.name</name><value>yarn</value></property></configuration>' > /opt/hadoop/etc/hadoop/mapred-site.xml && \
    echo '<configuration><property><name>yarn.nodemanager.aux-services</name><value>mapreduce_shuffle</value></property><property><name>yarn.nodemanager.resource.memory-mb</name><value>2048</value></property><property><name>yarn.scheduler.maximum-allocation-mb</name><value>1536</value></property></configuration>' > /opt/hadoop/etc/hadoop/yarn-site.xml

# Initialize HDFS data directories and format NameNode
RUN mkdir -p /opt/hadoop_tmp/hdfs/datanode /opt/hadoop_tmp/hdfs/namenode && \
    hdfs namenode -format -force

# Configure working directory and entrypoint
WORKDIR /workspace
COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

# Expose web monitor ports
EXPOSE 9870 9000 8088 4040 4041 4042 4043 4044 4045 8888

ENTRYPOINT ["/entrypoint.sh"]