# Fixing Memory Thrashing and Out-Of-Memory (OOM) in Ubuntu VM

## 1. Root Cause Analysis

When following the default VM tutorial with a 4 GB RAM allocation, the virtual machine becomes unresponsive due to memory starvation:

1. **Ubuntu Desktop Environment:** The GNOME desktop GUI consumes approximately 1.2 GB to 1.5 GB of RAM at idle.
2. **Multiple Hadoop JVMs:** Hadoop launches five independent Java processes:
   - `NameNode`
   - `DataNode`
   - `SecondaryNameNode`
   - `ResourceManager`
   - `NodeManager`
   By default, Java allows each process to request up to 1 GB of heap space, which can total ~5 GB.
3. **Spark and Python Workloads:** Starting `spark-shell` or running PySpark alongside a web browser requires an additional 1.5 GB to 2 GB.

When the combined memory demand exceeds the physical RAM without sufficient swap space, the Linux kernel encounters memory thrashing and the Out-Of-Memory (OOM) condition, locking up all I/O and user input.

---

## 2. Step 1: Increase VM Resources in VirtualBox

If your host machine has 16 GB of RAM, increase the VM allocation:

1. Power off the Ubuntu virtual machine completely.
2. Open **Oracle VM VirtualBox Manager**.
3. Select the VM and click **Settings** (or press `Ctrl + S`).
4. Navigate to **System** -> **Motherboard**:
   - Set **Base Memory** to `6144 MB` (6 GB) or `8192 MB` (8 GB).
5. Navigate to **System** -> **Processor**:
   - Set **Processor(s)** to `2` or `4` virtual CPUs (stay within the green zone).
6. Click **OK** and start the virtual machine.

---

## 3. Step 2: Restrict Hadoop Daemon Heap Memory

Cap the maximum heap memory allocated to each Hadoop daemon so they cannot exhaust system memory.

1. Open a terminal inside Ubuntu.
2. Open `/opt/hadoop/etc/hadoop/hadoop-env.sh` for editing:

```bash
sudo nano /opt/hadoop/etc/hadoop/hadoop-env.sh
```

3. Scroll to the bottom of the file and append the following configuration:

```bash
# Set global maximum heap size for Hadoop daemons
export HADOOP_HEAPSIZE_MAX=512m

# Configure individual daemon memory limits
export HADOOP_NAMENODE_OPTS="-Xms256m -Xmx512m $HADOOP_NAMENODE_OPTS"
export HADOOP_DATANODE_OPTS="-Xms256m -Xmx512m $HADOOP_DATANODE_OPTS"
export HADOOP_SECONDARYNAMENODE_OPTS="-Xms128m -Xmx256m $HADOOP_SECONDARYNAMENODE_OPTS"
export YARN_RESOURCEMANAGER_OPTS="-Xms256m -Xmx512m $YARN_RESOURCEMANAGER_OPTS"
export YARN_NODEMANAGER_OPTS="-Xms256m -Xmx512m $YARN_NODEMANAGER_OPTS"
```

4. Save and exit (`Ctrl + O`, `Enter`, then `Ctrl + X`).

---

## 4. Step 3: Limit YARN Container Resource Allocation

Prevent YARN from scheduling tasks that request more memory than the VM can provide.

1. Open `/opt/hadoop/etc/hadoop/yarn-site.xml` for editing:

```bash
sudo nano /opt/hadoop/etc/hadoop/yarn-site.xml
```

2. Ensure the `<configuration>` block contains the following memory constraints:

```xml
<configuration>
    <property>
        <name>yarn.nodemanager.aux-services</name>
        <value>mapreduce_shuffle</value>
    </property>
    <property>
        <name>yarn.nodemanager.auxservices.mapreduce.shuffle.class</name>
        <value>org.apache.hadoop.mapred.ShuffleHandler</value>
    </property>

    <!-- Maximum memory YARN can allocate across all containers -->
    <property>
        <name>yarn.nodemanager.resource.memory-mb</name>
        <value>1536</value>
    </property>

    <!-- Maximum memory for a single container request -->
    <property>
        <name>yarn.scheduler.maximum-allocation-mb</name>
        <value>1024</value>
    </property>

    <!-- Minimum memory for a single container request -->
    <property>
        <name>yarn.scheduler.minimum-allocation-mb</name>
        <value>256</value>
    </property>

    <!-- Disable strict virtual memory checking -->
    <property>
        <name>yarn.nodemanager.vmem-check-enabled</name>
        <value>false</value>
    </property>
</configuration>
```

3. Save and exit (`Ctrl + O`, `Enter`, then `Ctrl + X`).

---

## 5. Step 4: Create a Linux Swap File

A swap file acts as virtual memory on disk to prevent hard freezes during temporary memory spikes.

1. Create a 4 GB swap file:

```bash
sudo fallocate -l 4G /swapfile
```

2. Set the appropriate file permissions:

```bash
sudo chmod 600 /swapfile
```

3. Initialize the swap area:

```bash
sudo mkswap /swapfile
```

4. Activate the swap file:

```bash
sudo swapon /swapfile
```

5. Make the swap file permanent across reboots:

```bash
echo '/swapfile none swap sw 0 0' | sudo tee -a /etc/fstab
```

6. Verify the swap status:

```bash
free -h
```

---

## 6. Step 5: Limit Apache Spark Memory Usage

When launching Spark jobs or interactive shells, pass explicit memory arguments:

### For Spark Shell:
```bash
spark-shell --driver-memory 1g --executor-memory 1g
```

### For PySpark:
```bash
pyspark --driver-memory 1g --executor-memory 1g
```

### Global Default Configuration (Optional):
Create or edit `/opt/spark/conf/spark-defaults.conf`:

```bash
sudo cp /opt/spark/conf/spark-defaults.conf.template /opt/spark/conf/spark-defaults.conf
sudo nano /opt/spark/conf/spark-defaults.conf
```

Add these lines:

```properties
spark.driver.memory       1g
spark.executor.memory     1g
```

---

## 7. Step 6: Selective Service Execution

Do not start services you are not actively using:

- **HDFS Only (Recommended for Spark-only labs):**
  If your lab exercises only read and write data between HDFS and Spark, you only need storage daemons. Start only HDFS:
  ```bash
  start-dfs.sh
  ```
  *(This skips ResourceManager and NodeManager, saving ~1.5 GB of RAM).*

- **YARN Execution (When MapReduce or YARN scheduling is required):**
  ```bash
  start-yarn.sh
  ```

---

## 8. Restarting Services and Verification

1. Stop existing processes:

```bash
stop-yarn.sh
stop-dfs.sh
```

2. Start the optimized services:

```bash
start-dfs.sh
start-yarn.sh
```

3. Verify running JVM daemons:

```bash
jps
```

Expected output:
```text
NameNode
DataNode
SecondaryNameNode
ResourceManager
NodeManager
Jps
```

4. Monitor system memory usage in real time:

```bash
free -h
```
or
```bash
htop
```
