# Installing and Running Hadoop and Spark on a Virtual Machine with Ubuntu 26

**Course:** MTI850 – Analytiques des données massives  
**Author:** Alessandro L. Koerich  
**Version:** Ver. 4.0 – 02/09/2026  

---

## Overview

This tutorial will guide you to create your work environment for **MTI850 – Analytiques des données massives**.

### Summary of Steps:
1. Install and setup a virtual machine (VM) using Oracle's VirtualBox
2. Install Ubuntu LTS 26.04 (minimal installation)
3. Install Anaconda
4. Install Java 17 JDK and JRE
5. Install and setup Hadoop 3.5.0
6. Install and setup Apache Spark 4.2.0

> [!NOTE]
> It will take approximately **2 to 3 hours** to accomplish all these tasks.

---

## 1. Installing Oracle's VirtualBox

This is a walkthrough tutorial on how to install a virtual machine running Ubuntu Desktop 26.04 on your macOS, Windows, or Linux system using VirtualBox to provide a uniform configuration for all students.

### Requirements:
- **RAM:** 4 GB or more dedicated memory
- **Storage:** 30 GB of storage space (can be tweaked)

### Downloads:
- **Oracle VirtualBox 7.2.16:** Download from [https://www.virtualbox.org](https://www.virtualbox.org)
- **Ubuntu Desktop 26.04 LTS:** Download `ubuntu-26.04-desktop-amd64.iso` (~6.3 GB) from [https://ubuntu.com/download](https://ubuntu.com/download)

---

## 2. Creating a Virtual Machine

1. Open VirtualBox and click on **New** (or go to **Machine > New**).
2. Configure basic settings:
   - **Name:** `MTI850`
   - **ISO Image:** Select `ubuntu-26.04-desktop-amd64.iso`
   - **Type:** `Linux`
   - **Version:** `Ubuntu (64-bit)`
   - Check **Skip Unattended Installation**

> [!IMPORTANT]
> Make sure that hardware virtualization (**VT-x / AMD-V**) is enabled in your computer's BIOS/UEFI settings. Otherwise, the 64-bit option will not appear and the VM will fail to start.

3. **Memory Allocation:**
   - Set Base Memory to **4096 MB** (4 GB).

4. **Hard Disk Configuration:**
   - Select **Create a Virtual Hard Disk Now**.
   - **File Size:** `30.00 GB` or more.
   - **Hard Disk File Type:** `VDI (VirtualBox Disk Image)`.
   - **Storage on Physical Hard Disk:** `Dynamically allocated`.
   - Click **Create**.

---

## 3. Installing Ubuntu on your VM

1. Start the VM by clicking the green **Start** arrow (or right-click > **Start > Normal Start**).
2. If prompted, confirm the ISO file selection and click **Start**.
3. Choose installation options:
   - Select **Minimal (Default) Installation**.
   - Check **Download updates while installing Ubuntu**.
   - Click **Continue**.
4. **Disk Setup:**
   - Select **Erase disk and install Ubuntu** (this only wipes the virtual drive).
   - Click **Install Now** and confirm the changes.
5. **User Account Creation:**
   - **Username:** `mti850` *(strongly suggested, as subsequent tutorial steps assume this username)*.
   - Set your preferred name, computer name, and password.
6. Complete the installation and reboot the VM.

---

## 4. VirtualBox Guest Additions & System Packages

### 4.1 Guest Additions ISO
Before restarting, mount `VBoxGuestAdditions.iso` under VM **Settings > Storage** to access the virtual CD in Ubuntu.

### 4.2 Additional Packages
Start the Ubuntu VM, open a terminal, and install build tools:

```bash
sudo apt update
sudo apt install -y build-essential
```

Install the guest additions (`VBox_GA_7.0.20`) from the mounted Virtual CD drive.

---

## 5. Installing Anaconda

Open a terminal, download the Anaconda installer (~1.1 GB), and run it:

```bash
wget https://repo.anaconda.com/archive/Anaconda3-2026.07-1-Linux-x86_64.sh
chmod +x Anaconda3-2026.07-1-Linux-x86_64.sh
./Anaconda3-2026.07-1-Linux-x86_64.sh
```

During installation, when prompted to initialize Anaconda:
```text
Do you wish the installer to initialize Anaconda3 by running conda init? [yes|no]
[no] >>> yes
```

Apply changes to your current shell:
```bash
source ~/.bashrc
```

---

## 6. Installing Java

Hadoop requires Java to be installed. Hadoop runs smoothly with **Java 17** (newer versions may encounter compatibility issues).

### 6.1 Install OpenJDK 17

```bash
sudo apt update
sudo apt-get install -y openjdk-17-jre-headless
```

Verify the installation:
```bash
java -version
```
Expected output:
```text
openjdk version "17.0.6" 2025-07-15
OpenJDK Runtime Environment (build 17.0.16+8-Ubuntu-0ubuntu126.04)
OpenJDK 64-Bit Server VM (build 17.0.16+8-Ubuntu-0ubuntu126.04, mixed mode, sharing)
```

### 6.2 Set `JAVA_HOME`

Append `JAVA_HOME` to `~/.bashrc`:
```bash
echo "export JAVA_HOME=$(readlink -f /usr/bin/java | sed 's:bin/java::')" >> ~/.bashrc
source ~/.bashrc
```

Verify:
```bash
echo $JAVA_HOME
# Output: /usr/lib/jvm/java-17-openjdk-amd64/
```

---

## 7. Installing Hadoop

### 7.1 Download & Extract Hadoop 3.5.0 (~965 MB)

```bash
wget https://archive.apache.org/dist/hadoop/common/hadoop-3.5.0/hadoop-3.5.0.tar.gz
sudo tar -xvf hadoop-3.5.0.tar.gz -C /opt/
rm hadoop-3.5.0.tar.gz
cd /opt
sudo mv hadoop-3.5.0 hadoop
sudo chown mti850:mti850 -R hadoop
```

### 7.2 Configure Environment Variables

Add Hadoop environment variables to `~/.bashrc`:
```bash
echo "export HADOOP_HOME=/opt/hadoop" >> ~/.bashrc
echo "export PATH=\$PATH:\$HADOOP_HOME/bin:\$HADOOP_HOME/sbin" >> ~/.bashrc
source ~/.bashrc
```

Verify installation:
```bash
hadoop version
```

### 7.3 Configure `hadoop-env.sh`

Edit `/opt/hadoop/etc/hadoop/hadoop-env.sh`:
Find the line starting with `# export JAVA_HOME=` and edit/replace it with:
```bash
export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64/
```

---

## 8. Installing Apache Spark

### 8.1 Download & Extract Spark 4.2.0 (~543 MB)

Download Spark 4.2.0 (pre-built for Apache Hadoop 3.4 and later):

```bash
cd ~
wget https://dlcdn.apache.org/spark/spark-4.2.0/spark-4.2.0-bin-hadoop3.tgz
sudo tar -xvf spark-4.2.0-bin-hadoop3.tgz -C /opt/
rm spark-4.2.0-bin-hadoop3.tgz
cd /opt
sudo mv spark-4.2.0-bin-hadoop3 spark
sudo chown mti850:mti850 -R spark
```

### 8.2 Configure Environment Variables

Add Spark environment variables to `~/.bashrc`:
```bash
echo "export SPARK_HOME=/opt/spark" >> ~/.bashrc
echo "export PATH=\$PATH:\$SPARK_HOME/bin" >> ~/.bashrc
source ~/.bashrc
```

### 8.3 Verify Spark Installation

```bash
spark-shell --version
```

To test launching the interactive shell:
```bash
spark-shell
```

```text
      ____              __
     / __/__  ___ _____/ /__
    _\ \/ _ \/ _ `/ __/  '_/
   /___/ .__/\_,_/_/ /_/\_\   version 4.2.0
      /_/

Using Scala version 2.13.16, OpenJDK 64-Bit Server VM, 17.0.16
```

To quit `spark-shell`:
```scala
scala> :q
```

---

## 9. Configuring HDFS

To set up a local standalone/pseudo-distributed HDFS deployment, edit configuration files in `/opt/hadoop/etc/hadoop/`.

### 9.1 `core-site.xml`
Edit `/opt/hadoop/etc/hadoop/core-site.xml`:
```xml
<configuration>
  <property>
    <name>fs.defaultFS</name>
    <value>hdfs://localhost:9000</value>
  </property>
</configuration>
```

### 9.2 `hdfs-site.xml`
Edit `/opt/hadoop/etc/hadoop/hdfs-site.xml`:
```xml
<configuration>
  <property>
    <name>dfs.datanode.data.dir</name>
    <value>file:///opt/hadoop_tmp/hdfs/datanode</value>
  </property>
  <property>
    <name>dfs.namenode.name.dir</name>
    <value>file:///opt/hadoop_tmp/hdfs/namenode</value>
  </property>
  <property>
    <name>dfs.replication</name>
    <value>1</value>
  </property>
</configuration>
```

### 9.3 Create HDFS Storage Directories
```bash
sudo mkdir -p /opt/hadoop_tmp/hdfs/datanode
sudo mkdir -p /opt/hadoop_tmp/hdfs/namenode
sudo chown mti850:mti850 -R /opt/hadoop_tmp
```

### 9.4 `mapred-site.xml`
Edit `/opt/hadoop/etc/hadoop/mapred-site.xml`:
```xml
<configuration>
  <property>
    <name>mapreduce.framework.name</name>
    <value>yarn</value>
  </property>
</configuration>
```

### 9.5 `yarn-site.xml`
Edit `/opt/hadoop/etc/hadoop/yarn-site.xml`:
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
</configuration>
```

---

## 10. Configuring SSH

HDFS requires passwordless SSH access to `localhost`.

### 10.1 Install & Start OpenSSH Server
```bash
cd ~
which sshd || sudo apt install -y openssh-server
sudo systemctl status ssh
```

### 10.2 Generate and Authorize SSH Keys
Test connection:
```bash
ssh localhost
# Accept fingerprint (yes), then exit
exit
```

Generate passwordless RSA key pair and authorize it:
```bash
ssh-keygen -t rsa -P "" -f ~/.ssh/id_rsa
cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys
chmod 0600 ~/.ssh/authorized_keys
```

---

## 11. Formatting and Booting HDFS

> [!CAUTION]
> Formatting the NameNode will erase all data existing in HDFS. Only run this for initial setup.

### 11.1 Format NameNode
```bash
hdfs namenode -format -force
```

### 11.2 Start DFS and YARN Daemons
```bash
start-dfs.sh && start-yarn.sh
```

### 11.3 Verify Running Daemons
```bash
# If jps command is missing, install the full JDK:
sudo apt install -y openjdk-17-jdk-headless

jps
```

Expected active processes:
- `NameNode`
- `DataNode`
- `SecondaryNameNode`
- `ResourceManager`
- `NodeManager`
- `Jps`

### 11.4 Test HDFS Filesystem
```bash
hdfs dfs -mkdir /test
hdfs dfs -ls /
```
Expected output:
```text
Found 1 items
drwxr-xr-x   - mti850 supergroup          0 2026-09-02 19:27 /test
```

### 11.5 Web Monitoring Consoles
Bookmark these web interfaces in your browser:
- **YARN Resource Manager:** [http://localhost:8088](http://localhost:8088)
- **HDFS NameNode Web UI:** [http://localhost:9870](http://localhost:9870)

---

## 12. Working with Spark and HDFS

### 12.1 Download Sample Dataset & Upload to HDFS
Download "The Complete Works of William Shakespeare" from Project Gutenberg and copy it into HDFS:

```bash
cd ~
wget http://www.gutenberg.org/files/100/100-0.txt
hdfs dfs -put /home/mti850/100-0.txt /
```

### 12.2 Read and Analyze with Spark Shell
Launch `spark-shell`:
```bash
spark-shell
```

Execute Scala Spark commands:
```scala
val df = spark.read.text("hdfs://localhost:9000/100-0.txt")
df.show(10)
df.collect.foreach(println)
```

To exit:
```scala
scala> :q
```

### 12.3 Spark Web UI
While a Spark session or job is active, monitor execution at:
- **Spark Application UI:** [http://localhost:4040](http://localhost:4040) (or `http://localhost:4041` if 4040 is occupied)

---

## Conclusion
You are now fully set up with Hadoop 3.5.0, Spark 4.2.0, Anaconda, and Java 17 for MTI850!
