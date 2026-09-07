# MTI850 - Analytiques des donnees massives: Hadoop & Spark Environment

Containerized environment configured with Apache Hadoop 3.5.0 (HDFS and YARN), Apache Spark 4.2.0, PySpark, and JupyterLab for the MTI850 course.

---

## Table of Contents

- [Overview](#overview)
- [Prerequisites](#prerequisites)
- [Architecture and Port Mapping](#architecture-and-port-mapping)
- [Quick Start Guide](#quick-start-guide)
  - [1. Download Binaries (One-time)](#1-download-binaries-one-time)
  - [2. Build the Docker Image](#2-build-the-docker-image)
  - [3. Start the Environment](#3-start-the-environment)
  - [4. Access Web Interfaces](#4-access-web-interfaces)
- [Working with the Environment](#working-with-the-environment)
  - [Interactive Container Terminal](#interactive-container-terminal)
  - [Hadoop / HDFS Operations](#hadoop--hdfs-operations)
  - [Spark & PySpark Operations](#spark--pyspark-operations)
  - [JupyterLab Notebooks](#jupyterlab-notebooks)
- [Container Lifecycle Management](#container-lifecycle-management)
- [Documentation Index](#documentation-index)

---

## Overview

This repository provides a self-contained single-node cluster environment replacing traditional multi-gigabyte virtual machines with a lightweight Docker container. It includes pre-configured services for:
- **Hadoop 3.5.0** (HDFS NameNode, DataNode, SecondaryNameNode, YARN ResourceManager, NodeManager)
- **Spark 4.2.0** (PySpark, Spark Shell, Spark Submit)
- **Java 17 OpenJDK**
- **Python Virtual Environment** (`jupyterlab`, `pandas`, `numpy`, `findspark`)
- **Persistent Storage** mapped to `./workspace`

---

## Prerequisites

- **Docker Desktop** installed and running (with WSL 2 backend on Windows)
- **Docker Compose v2+**
- **Memory Allocation:** Minimum 4 GB RAM assigned to Docker / WSL 2
- **Shell:** Windows PowerShell or Unix Bash

---

## Architecture and Port Mapping

The container automatically starts all Hadoop daemons, YARN services, and JupyterLab on startup.

| Service | Container Port | Host Port | URL / Access Point | Notes |
| :--- | :--- | :--- | :--- | :--- |
| **JupyterLab Server** | 8888 | 8888 | http://localhost:8888 | Token: `mti850` |
| **HDFS NameNode Web UI** | 9870 | 9870 | http://localhost:9870 | Browse HDFS filesystem |
| **HDFS IPC / DefaultFS** | 9000 | 9000 | `hdfs://localhost:9000` | Native HDFS protocol |
| **YARN ResourceManager** | 8088 | 8088 | http://localhost:8088 | Job scheduling and monitoring |
| **Spark Application UI** | 4040 | 4040 | http://localhost:4040 | Active Spark application metrics |
| **Secondary Spark UIs** | 4041 - 4045 | 4041 - 4045 | http://localhost:4041+ | Additional Spark sessions |

The local `./workspace` directory is mounted at `/workspace` inside the container. All scripts, notebooks, and datasets placed in this folder persist across container rebuilds and restarts.

---

## Quick Start Guide

### 1. Download Binaries (One-time)

To optimize Docker build performance and avoid downloading large files during each image build, download the Hadoop and Spark archive files into the `downloads/` directory.

Run the PowerShell helper script:

```powershell
.\download_prerequisites.ps1
```

Or manually download the following archives and place them in the `downloads/` folder:
- **Hadoop 3.5.0:** `https://archive.apache.org/dist/hadoop/common/hadoop-3.5.0/hadoop-3.5.0.tar.gz`
- **Spark 4.2.0:** `https://dlcdn.apache.org/spark/spark-4.2.0/spark-4.2.0-bin-hadoop3.tgz`

### 2. Build the Docker Image

```bash
docker compose build
```

### 3. Start the Environment

```bash
docker compose up -d
```

### 4. Access Web Interfaces

- **JupyterLab:** Open http://localhost:8888 (Password / Token: `mti850`)
- **HDFS File Browser:** Open http://localhost:9870
- **YARN Cluster Overview:** Open http://localhost:8088

---

## Working with the Environment

### Interactive Container Terminal

To open a Bash terminal inside the running container:

```bash
docker exec -it mti850-hadoop-spark /bin/bash
```

To verify running Java daemons inside the container:

```bash
jps
```

Expected active processes: `NameNode`, `DataNode`, `SecondaryNameNode`, `ResourceManager`, `NodeManager`, `Jps`.

---

### Hadoop / HDFS Operations

Execute HDFS commands from inside the container:

```bash
# Check HDFS storage status and health
hdfs dfsadmin -report

# List the root directory of HDFS
hdfs dfs -ls /

# Create a user home directory
hdfs dfs -mkdir -p /user/root

# Upload a local file to HDFS
hdfs dfs -put /workspace/sample.txt /user/root/

# Read file contents from HDFS
hdfs dfs -cat /user/root/sample.txt
```

---

### Spark & PySpark Operations

#### 1. Interactive PySpark Shell
```bash
pyspark
```

Example PySpark test:
```python
df = spark.read.text("hdfs://localhost:9000/user/root/sample.txt")
df.show(5)
```

#### 2. Interactive Scala Spark Shell
```bash
spark-shell
```

```scala
val textFile = spark.read.textFile("hdfs://localhost:9000/user/root/sample.txt")
println(s"Lines count: ${textFile.count()}")
:q
```

#### 3. Submit a Spark Batch Job
```bash
spark-submit \
  --class org.apache.spark.examples.SparkPi \
  --master yarn \
  --deploy-mode client \
  $SPARK_HOME/examples/jars/spark-examples_*.jar 10
```

---

### JupyterLab Notebooks

1. Navigate to http://localhost:8888 in your web browser.
2. Enter token `mti850`.
3. Create a new Python 3 notebook.
4. Initialize PySpark with the following snippet:

```python
import findspark
findspark.init()

from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("MTI850-Notebook") \
    .master("local[*]") \
    .getOrCreate()

print("Spark version:", spark.version)
```

---

## Container Lifecycle Management

| Action | Command |
| :--- | :--- |
| View real-time container logs | `docker compose logs -f` |
| Stop services (preserve state) | `docker compose stop` |
| Resume stopped services | `docker compose start` |
| Restart services | `docker compose restart` |
| Stop and remove container | `docker compose down` |
| Rebuild image from scratch | `docker compose build --no-cache` |

---

## Documentation Index

Additional course guides and setup notes are located in the `documentation/` folder:

- [documentation/VM-Hadoop-Spark-Tutorial-4.0.md](documentation/VM-Hadoop-Spark-Tutorial-4.0.md) - Official course tutorial converted to Markdown (VirtualBox + Ubuntu 26 VM setup).
- [documentation/Docker-Hadoop-Spark-Setup.md](documentation/Docker-Hadoop-Spark-Setup.md) - Complete Docker environment architecture and configuration manual.
- [documentation/Fix-VM-Memory-OOM.md](documentation/Fix-VM-Memory-OOM.md) - Memory configuration and Out-Of-Memory (OOM) troubleshooting guide.
