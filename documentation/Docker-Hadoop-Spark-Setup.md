# Setting Up Hadoop 3.5 and Apache Spark 4.2 in Docker

## 1. Overview

This guide provides a complete Docker setup for **MTI850 – Analytiques des donnees massives**. 

Using Docker eliminates VirtualBox virtualization overhead and prevents Out-Of-Memory freezes by running headless, sharing system memory dynamically, and exposing all web monitors directly to your host operating system.

### Components Included:
- **Base OS:** Ubuntu 24.04 (minimal headless)
- **Java:** OpenJDK 17 JDK
- **Hadoop:** Apache Hadoop 3.5.0 (HDFS + YARN)
- **Spark:** Apache Spark 4.2.0 (PySpark + Scala Shell)
- **Python:** Python 3 with JupyterLab, PySpark bindings, Pandas, and NumPy
- **Persistence:** Bound `./workspace` volume for all scripts, notebooks, and datasets

---

## 2. Prerequisites

1. Install **Docker Desktop** on Windows or macOS:
   - [Docker Desktop Download](https://www.docker.com/products/docker-desktop/)
2. On Windows, ensure **WSL 2 backend** is enabled in Docker Desktop settings (**Settings** -> **General** -> **Use the WSL 2 based engine**).

---

## 3. Project Directory Structure

Your project directory should contain the following structure:

```text
mti850-hadoop-spark/
+-- downloads/
¦   +-- hadoop-3.5.0.tar.gz
¦   +-- spark-4.2.0-bin-hadoop3.tgz
+-- workspace/
+-- .gitignore
+-- Dockerfile
+-- docker-compose.yaml
+-- download_prerequisites.ps1
+-- entrypoint.sh
+-- README.md
```

---

## 4. Configuration Files

### File 1: `Dockerfile`

```dockerfile
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
ENV PYTHONPATH="${SPARK_HOME}/python:${PYTHONPATH}"
RUN pip install --no-cache-dir jupyterlab pandas numpy findspark

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
EXPOSE 9870 9000 8088 4040 8888

ENTRYPOINT ["/entrypoint.sh"]
```

---

### File 2: `entrypoint.sh`

```bash
#!/bin/bash

# Start SSH service
service ssh start

# Start HDFS and YARN daemons
start-dfs.sh
start-yarn.sh

# Start JupyterLab in the background (Access token: mti850)
jupyter lab --ip=0.0.0.0 --port=8888 --no-browser --allow-root --IdentityProvider.token='mti850' --ServerApp.allow_origin='*' &

# Keep container running and attach interactive shell
exec /bin/bash
```

> **Note for Windows users:** Ensure `entrypoint.sh` is saved with **LF (Unix)** line endings, not CRLF.

---

### File 3: `docker-compose.yaml`

```yaml
version: '3.8'

services:
  mti850-cluster:
    build: .
    container_name: mti850-hadoop-spark
    hostname: localhost
    restart: always
    ports:
      - "9870:9870"   # HDFS NameNode Web UI
      - "9000:9000"   # HDFS IPC Port
      - "8088:8088"   # YARN ResourceManager Web UI
      - "4040:4040"   # Spark Application Web UI
      - "8888:8888"   # JupyterLab Web UI
    volumes:
      - ./workspace:/workspace
    stdin_open: true
    tty: true
```

---

### File 4: `download_prerequisites.ps1`

```powershell
$downloadsDir = Join-Path $PSScriptRoot "downloads"
if (!(Test-Path $downloadsDir)) {
    New-Item -ItemType Directory -Path $downloadsDir | Out-Null
}

$hadoopUrl = "https://archive.apache.org/dist/hadoop/common/hadoop-3.5.0/hadoop-3.5.0.tar.gz"
$hadoopFile = Join-Path $downloadsDir "hadoop-3.5.0.tar.gz"

$sparkUrl = "https://dlcdn.apache.org/spark/spark-4.2.0/spark-4.2.0-bin-hadoop3.tgz"
$sparkFile = Join-Path $downloadsDir "spark-4.2.0-bin-hadoop3.tgz"

Write-Host "Checking prerequisites in $downloadsDir..."

if (!(Test-Path $hadoopFile)) {
    Write-Host "Downloading Hadoop 3.5.0 (this only happens once)..."
    curl.exe -L -o $hadoopFile $hadoopUrl
} else {
    Write-Host "Hadoop 3.5.0 archive already present."
}

if (!(Test-Path $sparkFile)) {
    Write-Host "Downloading Spark 4.2.0 (this only happens once)..."
    curl.exe -L -o $sparkFile $sparkUrl
} else {
    Write-Host "Spark 4.2.0 archive already present."
}

Write-Host "Ready to build Docker container: docker compose build"
```

---

## 5. Starting the Environment

1. Download the prerequisites (one-time step):
```powershell
.\download_prerequisites.ps1
```

2. Build and start the container:
```bash
docker compose up -d --build
```

3. Check container status:
```bash
docker ps
```

---

## 6. Accessing Services and Web Dashboards

All services are forwarded directly to `localhost` on your host browser:

| Service | URL | Description / Authentication |
| :--- | :--- | :--- |
| **JupyterLab** | `http://localhost:8888` | Token / Password: `mti850` |
| **HDFS NameNode UI** | `http://localhost:9870` | Browse HDFS filesystem & storage metrics |
| **YARN ResourceManager** | `http://localhost:8088` | Cluster jobs and resource status |
| **Spark Application UI** | `http://localhost:4040` | Active Spark DAG stages and task metrics |

---

## 7. Working Inside the Container

### Open an Interactive Terminal:
```bash
docker exec -it mti850-hadoop-spark /bin/bash
```

### Launch Interactive Spark Shell (Scala):
```bash
spark-shell
```

### Launch Interactive PySpark:
```bash
pyspark
```

### Test HDFS & Spark Integration (Tutorial Validation):

1. Inside the container terminal, download sample text:
```bash
wget http://www.gutenberg.org/files/100/100-0.txt -O /workspace/100-0.txt
```

2. Upload the file to HDFS:
```bash
hdfs dfs -put /workspace/100-0.txt /
```

3. Read and process the HDFS file using `spark-shell`:
```bash
spark-shell
```
Inside the Scala shell:
```scala
val df = spark.read.text("hdfs://localhost:9000/100-0.txt")
df.show(10)
:q
```

---

## 8. Managing and Stopping the Cluster

### Stop the cluster:
```bash
docker compose stop
```

### Start the cluster again:
```bash
docker compose start
```

### Completely remove the container:
```bash
docker compose down
```

### View container logs:
```bash
docker logs mti850-hadoop-spark
```

> **Data Persistence:** All notebooks, data files, and scripts stored in the `./workspace` directory are mapped to your host machine and persist permanently across container stops and restarts.
