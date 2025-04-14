# 📦 Number of Shipments Per Month — PySpark Solution 
**Language:** PySpark  

---

## 🧠 Problem Description

**Goal:**  
Calculate the number of **unique shipments per month** using the `amazon_shipment` table.

### Table Schema:
- `shipment_id`: ID of the shipment
- `sub_id`: Sub-component ID of a shipment
- `shipment_date`: Date of the shipment

Each shipment can have multiple `sub_id` entries, so a shipment's uniqueness is based on a combination of `shipment_id` and `sub_id`.

---

## 🛠️ Solution Approach

1. Extract the **year and month** from `shipment_date` using `date_format`.
2. Convert `shipment_id` and `sub_id` to strings to concatenate them into a unique identifier `uid`.
3. Group the data by the `year_month` and count the number of unique shipment components using `count(uid)`.
4. Convert the Spark DataFrame to a Pandas DataFrame for validation.

---

## ✅ PySpark Code

```python
# Import your libraries
import pyspark
from pyspark.sql.functions import date_format, col, concat, count
from pyspark.sql.types import StringType

# extract year-month from shipment_date
amazon_shipment = amazon_shipment.withColumn("year_month", date_format("shipment_date","yyy-MM"))

# create string versions of shipment_id and sub_id
amazon_shipment = amazon_shipment.withColumn("str_ship_id", col("shipment_id").cast(StringType())) \
                                 .withColumn("str_sub_id", col("sub_id").cast(StringType()))

# create a unique identifier for each shipment component
amazon_shipment = amazon_shipment.withColumn("uid", concat(amazon_shipment.str_ship_id, amazon_shipment.str_sub_id))

# count number of shipments per month
shipments_per_month = amazon_shipment.groupBy("year_month") \
                                     .agg(count("uid").alias("count"))

# convert to pandas dataframe for validation
shipments_per_month.toPandas()
