# Cheat Sheet: Lab 0 - Test Environment

Reference guide for environment smoke testing, Hadoop HDFS verification, SparkSession configuration, and JupyterLab validation.

---

## 1. SparkSession Initialization

Basic setup to initialize findspark and establish a SparkSession inside JupyterLab:

```python
import findspark
findspark.init()

from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Lab0-TestEnvironment") \
    .master("local[*]") \
    .getOrCreate()

print("Spark Version:", spark.version)
```

Configuration notes:
- `findspark.init()`: Locates the SPARK_HOME directory automatically and adds PySpark to sys.path.
- `master("local[*]")`: Utilizes all available CPU cores on the local single-node cluster.
- `getOrCreate()`: Reuses an active session if one exists, preventing duplicate driver processes.

---

## 2. In-Memory DataFrame Creation

Creating simple DataFrames directly from Python lists and schemas:

```python
# Create DataFrame from a list of values
data = [("Alice", 25), ("Bob", 30), ("Charlie", 35)]
columns = ["Name", "Age"]

df = spark.createDataFrame(data, columns)
df.show()
df.printSchema()
```

---

## 3. Hadoop HDFS File System Operations

Interacting with files stored in the distributed file system (HDFS):

```python
# Read plain text from HDFS
hdfs_path = "hdfs://localhost:9000/user/root/sample.txt"
text_df = spark.read.text(hdfs_path)

# Preview rows
text_df.show(5, truncate=False)

# Count records
total_lines = text_df.count()
first_line = text_df.first()[0]
```

CLI commands reference (inside container or host):
```bash
# Verify HDFS health report
hdfs dfsadmin -report

# List HDFS directory contents
hdfs dfs -ls /user/root/

# Copy local file to HDFS
hdfs dfs -put /workspace/sample.txt /user/root/

# Display file contents from HDFS
hdfs dfs -cat /user/root/sample.txt
```

---

## 4. Basic Word Counting Transformation

Simple line splitting and word counting using Spark functions:

```python
from pyspark.sql.functions import split, explode, col

words_df = text_df.select(
    explode(split(col("value"), " ")).alias("word")
).filter(col("word") != "")

print("Total words:", words_df.count())
```

---

## 5. Visualizations and LaTeX Rendering

Checking Matplotlib plotting and LaTeX mathematical typesetting:

```python
import matplotlib.pyplot as plt

# Matplotlib plot validation
plt.figure(figsize=(6, 3))
plt.plot([1, 2, 3, 4], [1, 4, 9, 16], 'r-o')
plt.title("Matplotlib Integration Test")
plt.xlabel("X")
plt.ylabel("Y")
plt.grid(True)
plt.show()
```

Markdown LaTeX validation cell:
```markdown
Inline formula: $E = mc^2$
Display block formula:
$$f(x) = \frac{1}{\sigma \sqrt{2\pi}} e^{-\frac{1}{2}\left(\frac{x - \mu}{\sigma}\right)^2}$$
```

---

## 6. Resource Cleanup

Stopping the Spark driver application when work is complete:

```python
spark.stop()
```
