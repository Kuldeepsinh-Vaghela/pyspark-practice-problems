# 📊 Unique Users Per Client Per Month — PySpark Solution 
**Language:** PySpark  

---

## 🧠 Problem Description

**Goal:**  
Find the number of unique users per client for each month.

You are given a table `fact_events` with the following schema:
- `user_id`: ID of the user
- `client_id`: ID of the client
- `time_id`: Timestamp of the event

Your task is to write a PySpark query to return the number of **unique users** per `client_id` per **month**.

---

## 🛠️ Solution Approach

1. Extract the month from the `time_id` column.
2. Group the data by `client_id` and extracted `month`.
3. Count distinct `user_id`s in each group.
4. Convert the resulting Spark DataFrame to a Pandas DataFrame for validation.

---

## ✅ PySpark Code

```python
# Import your libraries
import pyspark
from pyspark.sql.functions import month, year, col, count_distinct

# getting month from date
fact_events = fact_events.withColumn("month", month("time_id"))

# group by client_id and month, and count distinct users
final_df = fact_events.groupBy("client_id", "month") \
                      .agg(count_distinct("user_id").alias("users_num"))

# convert to pandas dataframe to validate
final_df.toPandas()
