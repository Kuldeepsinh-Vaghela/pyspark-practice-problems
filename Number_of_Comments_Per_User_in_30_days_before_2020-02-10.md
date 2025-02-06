# PySpark Task: Calculate Total Number of Comments Received in the Last 30 Days

## Problem Statement:
Return the total number of comments received for each user in the **30 or fewer days before 2020-02-10**. Don't output users who haven't received any comment in the defined time period.

### Input Table:
| user_id | created_at | number_of_comments |
|---------|------------|--------------------|
| 18      | 2019-12-29 | 1                  |
| 25      | 2019-12-21 | 1                  |
| 78      | 2020-01-04 | 1                  |
| 37      | 2020-02-01 | 1                  |
| 99      | 2020-02-02 | 1                  |
| 18      | 2020-01-31 | 1                  |
| 58      | 2020-01-26 | 1                  |
| 32      | 2020-01-10 | 1                  |
| 24      | 2020-02-03 | 1                  |
| 8       | 2020-02-10 | 1                  |
| 18      | 2020-02-03 | 1                  |
| 50      | 2020-01-31 | 1                  |

## PySpark Solution:

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum
from pyspark.sql.types import DateType

# Initialize Spark session
spark = SparkSession.builder.appName("CommentCount").getOrCreate()

# Sample Data
data = [
    (18, "2019-12-29", 1), (25, "2019-12-21", 1), (78, "2020-01-04", 1), 
    (37, "2020-02-01", 1), (99, "2020-02-02", 1), (18, "2020-01-31", 1), 
    (58, "2020-01-26", 1), (32, "2020-01-10", 1), (24, "2020-02-03", 1), 
    (8, "2020-02-10", 1), (18, "2020-02-03", 1), (50, "2020-01-31", 1)
]

# Define schema
columns = ["user_id", "created_at", "number_of_comments"]

# Create DataFrame
df = spark.createDataFrame(data, columns)

# Convert 'created_at' to DateType
df = df.withColumn("created_at", col("created_at").cast(DateType()))

# Define the date range: 30 days before 2020-02-10 (i.e., from 2020-01-11 to 2020-02-09)
start_date = "2020-01-11"
end_date = "2020-02-09"

# Filter comments within the 30-day window
filtered_df = df.filter((col("created_at") >= start_date) & (col("created_at") <= end_date))

# Aggregate total comments per user
result_df = filtered_df.groupBy("user_id").agg(sum("number_of_comments").alias("total_comments"))

# Show results
result_df.show()

