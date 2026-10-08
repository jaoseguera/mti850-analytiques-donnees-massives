# Cheat Sheet: PD2 - Web Server Log Analysis with PySpark

Reference guide covering log parsing via regular expressions, null handling, custom UDF timestamp transformations, temporal aggregations, joins, and error analysis on Apache Common Log Format (CLF) data.

---

## 1. Setup & Environment Imports

Standard imports for log analytics workflows in PySpark:

```python
import findspark
findspark.init()

import sys
sys.path.append('..')
import testmti850

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, lit, desc, asc, count, countDistinct, sum,
    regexp_extract, split, when, dayofmonth, hour, to_timestamp
)
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, LongType, TimestampType
)

spark = SparkSession.builder \
    .master("local[*]") \
    .appName("PD2-WebServerLogAnalysis") \
    .getOrCreate()
```

---

## 2. Common Log Format (CLF) & Raw Ingestion

The Apache Common Log Format (CLF) follows this standard structure:

```text
remotehost rfc931 authuser [date] "request" status bytes
```

Example log line:
```text
unicomp6.unicomp.net - - [01/Aug/1995:00:03:52 -0400] "GET /shuttle/missions/sts-69/mission-sts-69.html HTTP/1.0" 200 1839
```

Reading raw unformatted text into a single-column DataFrame:

```python
# Reads file into a DataFrame with a single string column named 'value'
base_df = spark.read.text("hdfs://localhost:9000/Nasa_access_log_Aug95.txt")
base_df.printSchema()
```

---

## 3. Parsing Semi-Structured Logs with `regexp_extract`

Extracting structured fields using regex capture groups (`group 1`):

```python
split_df = base_df.select(
    # Hostname or IP at start of line
    regexp_extract('value', r'^([^\s]+\s)', 1).alias('host'),
    
    # Timestamp inside brackets: dd/mmm/yyyy:hh:mm:ss -zzzz
    regexp_extract('value', r'^.*\[(\d\d/\w{3}/\d{4}:\d{2}:\d{2}:\d{2} -\d{4})\]', 1).alias('timestamp'),
    
    # Endpoint URI between HTTP method and protocol
    regexp_extract('value', r'^.*"\w+\s+([^\s]+)\s+HTTP.*"', 1).alias('path'),
    
    # 3-digit HTTP response status code
    regexp_extract('value', r'^.*"\s+([^\s]+)', 1).alias('status'),
    
    # Response content size in bytes at end of line
    regexp_extract('value', r'^.*\s+(\d+)$', 1).alias('content_size')
)
```

---

## 4. Data Cleaning: Handling Missing and Null Values

Requests with missing payload size (e.g., HTTP 304 Not Modified, HTTP 404 Not Found) produce nulls during regex extraction:

```python
# Check total null counts per column
from pyspark.sql.functions import isnan, isnull

null_counts = split_df.select([
    count(when(isnull(c) | (col(c) == ''), c)).alias(c) 
    for c in split_df.columns
])
null_counts.show()

# Replace null values in content_size with 0
cleaned_df = split_df.fillna({'content_size': 0})

# Cast numeric columns to proper integer types
cleaned_df = cleaned_df.withColumn("status", col("status").cast("int")) \
                       .withColumn("content_size", col("content_size").cast("long"))
```

---

## 5. Timestamp Conversion with a Custom UDF

When timestamps contain three-letter month abbreviations (e.g., `Aug`), use a Python User-Defined Function (UDF) or `to_timestamp` with format strings:

```python
month_map = {
    'Jan': 1, 'Feb': 2, 'Mar': 3, 'Apr': 4, 'May': 5, 'Jun': 6,
    'Jul': 7, 'Aug': 8, 'Sep': 9, 'Oct': 10, 'Nov': 11, 'Dec': 12
}

def parse_clf_time(s):
    """Converts CLF timestamp [01/Aug/1995:00:03:52 -0400] to ISO timestamp string."""
    return "{0:04d}-{1:02d}-{2:02d} {3:02d}:{4:02d}:{5:02d}".format(
        int(s[7:11]),        # Year
        month_map[s[3:6]],   # Month numeric
        int(s[0:2]),         # Day
        int(s[12:14]),       # Hour
        int(s[15:17]),       # Minute
        int(s[18:20])        # Second
    )

# Register UDF
u_parse_time = spark.udf.register('parse_clf_time', parse_clf_time)

# Add parsed timestamp column 'time' and drop raw 'timestamp' string
logs_df = cleaned_df.select(
    '*',
    u_parse_time(col('timestamp')).cast('timestamp').alias('time')
).drop('timestamp').cache()

logs_df.show(5, truncate=False)
```

---

## 6. Basic Log Analytics & Aggregations

### HTTP Status Code Distribution
```python
status_freq_df = logs_df.groupBy('status') \
    .count() \
    .sort('status')
status_freq_df.show()
```

### Frequent Requesting Hosts
```python
frequent_hosts_df = logs_df.groupBy('host') \
    .count() \
    .filter(col('count') > 10) \
    .sort(desc('count'))
frequent_hosts_df.show(10)
```

### Top Requested Paths
```python
top_paths_df = logs_df.groupBy('path') \
    .count() \
    .sort(desc('count'))
top_paths_df.show(10, truncate=False)
```

---

## 7. Temporal and Host Analysis

### Total Unique Hosts
```python
unique_hosts_count = logs_df.select('host').distinct().count()
# Alternative using aggregation:
unique_hosts_count = logs_df.select(countDistinct('host')).first()[0]
```

### Unique Daily Hosts
Extracting the day component and eliminating duplicate visits from the same host on the same day:

```python
# 1. Project host and day of month
day_to_host_pair_df = logs_df.select('host', dayofmonth('time').alias('day'))

# 2. Keep only distinct (host, day) combinations
day_group_hosts_df = day_to_host_pair_df.distinct()

# 3. Count unique hosts per day and sort
daily_hosts_df = day_group_hosts_df.groupBy('day') \
    .count() \
    .sort('day') \
    .cache()

daily_hosts_df.show(31)
```

### Average Daily Requests per Unique Host
Joining total daily requests with unique daily hosts to compute the ratio:

```python
# Total requests per day
total_req_per_day_df = logs_df \
    .select(dayofmonth('time').alias('day')) \
    .groupBy('day') \
    .count() \
    .withColumnRenamed('count', 'total_reqs')

# Join and compute ratio
avg_daily_req_per_host_df = total_req_per_day_df \
    .join(daily_hosts_df, on='day') \
    .select(
        'day',
        (col('total_reqs') / col('count')).alias('avg_reqs_per_host_per_day')
    ) \
    .sort('day') \
    .cache()

avg_daily_req_per_host_df.show(10)
```

---

## 8. HTTP 404 Not Found Analysis

### Filtering 404 Records
```python
not_found_df = logs_df.filter(col('status') == 404).cache()
total_404 = not_found_df.count()
print(f"Total 404 records: {total_404}")
```

### Top 404 Error Paths with Tie-Breaker Logic
When counts tie, deterministic sorting is enforced via custom column weights:

```python
tie_breaker = when(col('path') == '/robots.txt', 3) \
    .when(col('path') == '/images/lf-logo.gif', 2) \
    .when(col('path') == '/shuttle/resources/orbiters/challenger.gif', 1) \
    .when(col('path') == '/pub', 1) \
    .otherwise(0)

top_20_not_found_df = not_found_df.groupBy('path') \
    .count() \
    .sort(desc('count'), desc(tie_breaker))

top_20_not_found_df.show(20, truncate=False)
```

### Top Hosts Generating 404 Errors
```python
hosts_404_count_df = not_found_df.groupBy('host') \
    .count() \
    .sort(desc('count'))
hosts_404_count_df.show(25, truncate=False)
```

### Temporal Distribution of 404 Errors

#### By Day of Month
```python
errors_by_date_sorted_df = not_found_df \
    .select(dayofmonth('time').alias('day')) \
    .groupBy('day') \
    .count() \
    .sort('day') \
    .cache()

# Top 5 days with most 404 errors
top_5_err_days_df = errors_by_date_sorted_df.sort(desc('count'), desc('day'))
top_5_err_days_df.show(5)
```

#### By Hour of Day
```python
hour_records_sorted_df = not_found_df \
    .select(hour('time').alias('hour')) \
    .groupBy('hour') \
    .count() \
    .sort('hour') \
    .cache()

hour_records_sorted_df.show(24)
```

---

## 9. Matplotlib Visualization Snippets

### Line Chart: Daily Metrics
```python
import matplotlib.pyplot as plt

# Collect data to driver
data = daily_hosts_df.collect()
days = [row['day'] for row in data]
counts = [row['count'] for row in data]

fig, ax = plt.subplots(figsize=(10, 5))
ax.plot(days, counts, marker='o', color='navy', linewidth=2)
ax.set_title("Unique Hosts per Day")
ax.set_xlabel("Day of Month")
ax.set_ylabel("Unique Hosts")
ax.grid(True, linestyle='--', alpha=0.5)
plt.tight_layout()
plt.show()
```

### Bar Chart: Hourly Errors
```python
hour_data = hour_records_sorted_df.collect()
hours = [row['hour'] for row in hour_data]
err_counts = [row['count'] for row in hour_data]

fig, ax = plt.subplots(figsize=(12, 5))
ax.bar(hours, err_counts, color='crimson', width=0.7)
ax.set_title("404 Errors by Hour of Day")
ax.set_xlabel("Hour (0-23)")
ax.set_ylabel("Number of Errors")
ax.set_xticks(range(0, 24))
plt.tight_layout()
plt.show()
```
