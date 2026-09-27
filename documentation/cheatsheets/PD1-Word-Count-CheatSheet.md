# Cheat Sheet: PD1 - Text Analysis & Distributed Word Count

Reference guide covering string manipulations, regex normalization, array exploding, group aggregations, and the end-to-end distributed Word Count pipeline with PySpark.

---

## 1. Setup & Environment Imports

Standard boilerplate for assignment notebooks:

```python
import findspark
findspark.init()

import sys
sys.path.append('..')

import testmti850
import utilmti850

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, lit, concat, length, lower, 
    regexp_replace, split, explode, mean, desc
)

spark = SparkSession.builder \
    .master("local[*]") \
    .appName("PD1-WordCount") \
    .getOrCreate()
```

---

## 2. Basic DataFrame String Transformations

### Appending Characters / Suffixes
Concatenating literal values to existing string columns:

```python
wordsDF = spark.createDataFrame([("cat",), ("elephant",), ("rat",)], ["word"])

# Append 's' to each word using concat and lit
pluralDF = wordsDF.select(concat(col("word"), lit("s")).alias("word"))
pluralDF.show()
```

### Computing String Length
Calculating character length of strings in a DataFrame:

```python
# Compute length of each word
pluralLengthsDF = pluralDF.select(col("word"), length(col("word")).alias("length"))
pluralLengthsDF.show()
```

---

## 3. Counting, Grouping, and Aggregation Functions

### Word Frequency Counting
```python
wordsDF = spark.createDataFrame([
    ("cat",), ("elephant",), ("rat",), ("rat",), ("cat",)
], ["word"])

# Group by word and count occurrences
wordCountsDF = wordsDF.groupBy("word").count()
wordCountsDF.show()
```

### Counting Unique Words
```python
# The number of distinct rows in wordCountsDF represents the unique word count
uniqueWordsCount = wordCountsDF.count()
print("Unique words:", uniqueWordsCount)
```

### Calculating the Mean of Group Counts
Extracting aggregate scalar values from a DataFrame:

```python
# Calculate average occurrences across all words
averageCount = wordCountsDF.select(mean("count")).first()[0]
print("Average word count:", round(averageCount, 2))
```

---

## 4. Reusable Word Count Function

Modular function to compute word frequencies from an arbitrary DataFrame with a `"word"` column:

```python
def wordCount(wordListDF):
    """
    Computes frequency of each word in a DataFrame.
    
    Args:
        wordListDF: DataFrame containing a single string column named 'word'
        
    Returns:
        DataFrame with columns 'word' and 'count'
    """
    return wordListDF.groupBy("word").count()
```

---

## 5. Text Normalization & Cleaning Pipeline

### Stripping Punctuation with Regular Expressions
Standard regex pattern to remove punctuation while preserving words and whitespace:

```python
def removePunctuation(column):
    """
    Removes punctuation and special characters from a string column.
    Pattern r"[^\w\s]" matches any character that is not a word character or whitespace.
    """
    return lower(regexp_replace(column, r"[^\w\s]", ""))

# Example test
raw_df = spark.createDataFrame([("Hello, World! Big-Data analytics?",)], ["text"])
clean_df = raw_df.select(removePunctuation(col("text")).alias("clean_text"))
# Result: "hello world bigdata analytics"
```

### Tokenization & Row Exploding
Converting full text lines into individual word rows:

```python
# 1. Split text into an array of words
# 2. Explode the array so each element becomes an individual row
# 3. Filter out empty strings caused by multiple consecutive spaces
wordsDF = clean_df.select(
    explode(split(col("clean_text"), r"\s+")).alias("word")
).filter(col("word") != "")
```

---

## 6. End-to-End Pipeline for Large Text Files

Processing a complete book dataset (e.g., Shakespeare works in `100-0.txt`):

```python
# 1. Load raw text file from storage
raw_text_df = spark.read.text("workspace/100-0.txt")

# 2. Clean lines, tokenize, and explode into words
clean_words_df = raw_text_df.select(
    explode(
        split(removePunctuation(col("value")), r"\s+")
    ).alias("word")
).filter(length(col("word")) > 0)

# 3. Cache processed words if multiple queries follow
clean_words_df.cache()

# 4. Count total words
total_words = clean_words_df.count()

# 5. Compute word frequencies and order descending
topWordsAndCountsDF = clean_words_df.groupBy("word") \
    .count() \
    .orderBy(desc("count"))

# 6. Retrieve top 15 most frequent words
top15 = topWordsAndCountsDF.take(15)
for word, count in top15:
    print(f"{word}: {count}")
```

---

## 7. Verification and Visualization Utilities

### Testing with `testmti850`
```python
testmti850.Test.assertEquals(uniqueWordsCount, 3, "incorrect unique words count")
testmti850.Test.assertEquals(round(averageCount, 2), 1.67, "incorrect average count")
```

### Plotting Word Distributions with `utilmti850`
```python
import matplotlib.pyplot as plt

# Extract top words and counts as Python lists
top_words = [row['word'] for row in top15]
top_counts = [row['count'] for row in top15]

# Render bar chart layout using utilmti850 template
fig, ax = utilmti850.prepareSubplot(
    xticks=range(len(top_words)), 
    yticks=range(0, max(top_counts) + 1000, 5000),
    figsize=(12, 6)
)

ax.bar(range(len(top_words)), top_counts, color="#1f77b4")
ax.set_xticklabels(top_words, rotation=45)
ax.set_ylabel("Occurrences")
ax.set_title("Top 15 Most Frequent Words")
plt.tight_layout()
plt.show()
```
