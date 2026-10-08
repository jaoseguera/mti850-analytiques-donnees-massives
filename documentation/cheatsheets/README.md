# Course Notebooks Cheat Sheets

Reference guides and syntax cheatsheets for the MTI850 practical notebooks:

- [Lab 0: Environment Smoke Test](Lab0-TestEnvironment-CheatSheet.md)
  - SparkSession setup with findspark
  - Reading text from HDFS
  - In-memory DataFrame creation
  - Matplotlib & LaTeX verification

- [Math and Python Review](MathPythonReview-CheatSheet.md)
  - Linear algebra (vectors, matrices, dot products, matrix inverses)
  - NumPy arrays, slicing, and stacking
  - PySpark DenseVector
  - Python functional programming (lambdas, map, filter, reduce)
  - Unit testing with testmti850

- [Practical Spark Tutorial](Tutoriel-Spark-Pratique-CheatSheet.md)
  - RDD fundamentals (actions vs transformations, classic word count, caching)
  - Modern PySpark DataFrames API (schema, columns, filtering, aggregations)
  - Spark SQL temporary views and queries
  - Big data storage: CSV vs Parquet

- [PD1: Text Analysis and Word Count](PD1-Word-Count-CheatSheet.md)
  - String manipulation (concat, length)
  - Group frequency counts, unique words, and group means
  - Text normalization pipeline (regex punctuation stripping, lowercasing, tokenization, explode)
  - Large-scale text analysis on complete works of Shakespeare
  - Visualization with utilmti850

- [PD2: Web Server Log Analysis](PD2-Web-Server-Log-CheatSheet.md)
  - Common Log Format (CLF) parsing via regex capture groups with regexp_extract
  - Data cleaning and missing value handling with fillna
  - Custom UDF timestamp conversions to TimestampType
  - Daily and hourly temporal aggregations (dayofmonth, hour)
  - Join operations to compute average daily requests per unique host
  - In-depth HTTP 404 error analysis with custom tie-breaker sorting

- [PD3: Power Plant Output Prediction & Spark ML](PD3-Power-Plant-ML-CheatSheet.md)
  - Custom StructType schema definition for continuous variables
  - Feature engineering and dense vector generation with VectorAssembler
  - ML Pipeline construction, training, and coefficient extraction
  - Evaluation metrics (RMSE, R2) and residual error analysis under Gaussian assumptions
  - Hyperparameter tuning via ParamGridBuilder and 3-fold CrossValidator
  - Algorithm comparison: Linear Regression vs Decision Trees vs Random Forests
