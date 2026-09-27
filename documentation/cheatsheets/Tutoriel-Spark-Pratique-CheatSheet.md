# Cheat Sheet: Practical Spark & PySpark DataFrames

Reference guide covering Spark RDD fundamentals, modern PySpark DataFrame operations, Spark SQL queries, caching strategies, and Parquet storage formats.

---

## 1. Spark Architecture & Initialization

```python
import findspark
findspark.init()

from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Spark-Practical-Tutorial") \
    .master("local[*]") \
    .config("spark.executor.memory", "1g") \
    .getOrCreate()

sc = spark.sparkContext
```

Key Concepts:
- **Driver**: Process executing the user's `main()` function, creating the `SparkContext`/`SparkSession`, and coordinating job execution.
- **Executors**: Worker processes distributed across cluster nodes that execute assigned computation tasks and store data partitions.
- **Lazy Evaluation**: Transformations are only computed when an Action requires a concrete result.

---

## 2. RDD API: Transformations vs Actions

### Core RDD Operations
```python
# Create RDD from text file
lines_rdd = sc.textFile("workspace/sample.txt")

# Transformations (Lazy - returns a new RDD)
filtered_rdd = lines_rdd.filter(lambda line: len(line.strip()) > 0)
words_rdd = filtered_rdd.flatMap(lambda line: line.split(" "))
mapped_rdd = words_rdd.map(lambda word: (word.lower(), 1))

# Actions (Triggers execution - returns concrete values to driver)
total_count = lines_rdd.count()
first_element = lines_rdd.first()
top_five = mapped_rdd.take(5)
all_collected = mapped_rdd.collect()  # Caution: transfers entire dataset to driver memory!
```

### Classic RDD Word Count
```python
counts_rdd = sc.textFile("workspace/sample.txt") \
    .flatMap(lambda line: line.split(" ")) \
    .filter(lambda word: word != "") \
    .map(lambda word: (word.lower(), 1)) \
    .reduceByKey(lambda a, b: a + b) \
    .sortBy(lambda pair: pair[1], ascending=False)

print(counts_rdd.take(10))
```

### Caching and Persistence
```python
from pyspark import StorageLevel

# Default in-memory cache
lines_rdd.cache()

# Specific persistence level
lines_rdd.persist(StorageLevel.MEMORY_AND_DISK)

# Release memory
lines_rdd.unpersist()
```

---

## 3. PySpark DataFrame API (Modern Workflow)

### DataFrame Creation and Schema Inspection
```python
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

# From Python list of tuples
data = [("Engineering", "Alice", 75000), ("Engineering", "Bob", 82000), ("HR", "Carol", 60000)]
df = spark.createDataFrame(data, ["Department", "Name", "Salary"])

# Schema inspection
df.printSchema()
df.columns      # List of column names
df.dtypes       # List of (column_name, data_type) pairs
df.describe().show()  # Statistical summary (count, mean, stddev, min, max)
```

### Selection, Column Operations, and Renaming
```python
from pyspark.sql.functions import col, lit, round

# Projection
df.select("Name", "Salary").show()
df.select(col("Name"), (col("Salary") * 1.05).alias("New_Salary")).show()

# Add a new column
df = df.withColumn("Bonus", col("Salary") * 0.10)

# Add constant literal
df = df.withColumn("Company", lit("TechCorp"))

# Rename a column
df = df.withColumnRenamed("Salary", "Base_Salary")

# Drop column
df = df.drop("Company")
```

### Filtering and Conditions
```python
# Single condition
high_earners = df.filter(col("Base_Salary") > 70000)

# Multiple conditions (requires parentheses and bitwise operators &, |, ~)
filtered = df.filter((col("Department") == "Engineering") & (col("Base_Salary") >= 80000))

# String filtering
like_df = df.filter(col("Name").startswith("A"))
```

### Grouping and Aggregations
```python
from pyspark.sql.functions import count, avg, min, max, sum

dept_stats = df.groupBy("Department").agg(
    count("*").alias("Employee_Count"),
    round(avg("Base_Salary"), 2).alias("Avg_Salary"),
    min("Base_Salary").alias("Min_Salary"),
    max("Base_Salary").alias("Max_Salary"),
    sum("Base_Salary").alias("Total_Payroll")
).orderBy(col("Avg_Salary").desc())

dept_stats.show()
```

---

## 4. Spark SQL Integration

Execute ANSI SQL queries against DataFrames by registering temporary views:

```python
# Register temporary view (scoped to SparkSession)
df.createOrReplaceTempView("employees")

# Execute SQL query
sql_result = spark.sql("""
    SELECT 
        Department,
        COUNT(*) as Headcount,
        ROUND(AVG(Base_Salary), 2) as Average_Salary
    FROM employees
    WHERE Base_Salary > 50000
    GROUP BY Department
    ORDER BY Average_Salary DESC
""")

sql_result.show()
```

---

## 5. Big Data Storage: CSV vs Parquet

| Characteristic | CSV | Parquet |
| :--- | :--- | :--- |
| **Storage Layout** | Row-oriented | Columnar |
| **Schema Preservation** | No (requires inference) | Yes (embedded schema & types) |
| **Compression** | Poor / optional gzip | Built-in per-column (Snappy, GZIP) |
| **Query Performance** | Reads full row | Reads only requested columns |
| **Predicate Pushdown** | Unsupported | Supported (skips unnecessary data blocks) |

### Writing and Reading Files
```python
# Write DataFrame to Parquet
df.write.mode("overwrite").parquet("workspace/data/employees.parquet")

# Read DataFrame from Parquet
parquet_df = spark.read.parquet("workspace/data/employees.parquet")

# Write to CSV with header
df.write.mode("overwrite").option("header", "true").csv("workspace/data/employees.csv")
```
