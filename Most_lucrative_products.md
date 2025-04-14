# 💰 Most Lucrative Products — PySpark Solution
**Language:** PySpark  

---

## 🧠 Problem Description

**Goal:**  
Identify the top 5 products with the highest revenue generated during the **first half of 2022** (January to June).

### Table: `online_orders`  
- `product_id`: Unique ID of the product  
- `date_sold`: Date the product was sold  
- `units_sold`: Number of units sold  
- `cost_in_dollars`: Price per unit of the product  

---

## 🛠️ Solution Approach

1. **Extract Month:** Use `month()` to extract the month from `date_sold`.
2. **Compute Revenue:** Calculate revenue as `cost_in_dollars * units_sold`.
3. **Filter by Date:** Keep only records from the first half of 2022 (`month <= 6`).
4. **Group and Aggregate:** Group by `product_id` and calculate the total revenue.
5. **Sort and Limit:** Sort products by revenue in descending order and pick the top 5.
6. **Validate:** Convert final DataFrame to Pandas for result display or testing.

---

## ✅ PySpark Code

```python
# Import your libraries
import pyspark
from pyspark.sql.functions import month, col, sum, desc

# extracting month and calculating revenue
online_orders = online_orders.withColumn("month", month("date_sold")) \
                             .withColumn("revenue", online_orders.cost_in_dollars * online_orders.units_sold)

# filter for first half of 2022
first_half_online_orders = online_orders.filter(col("month") <= 6)

# aggregate total revenue by product
grouped_df = first_half_online_orders.groupBy("product_id") \
                                     .agg(sum("revenue").alias("revenue"))

# get top 5 products by revenue
grouped_df = grouped_df.sort(grouped_df.revenue.desc()).limit(5)

# convert to pandas dataframe for validation
grouped_df.toPandas()
