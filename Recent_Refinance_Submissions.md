## Question

Given the following dataset with columns `d`, `user_id`, `created_at`, `status`, and `type`:

| d  | user_id | created_at | status                    | type     |
|----|---------|------------|---------------------------|----------|
| 1  | 100     | 2017-04-21 | prequal_completd_offer     | Refinance|
| 2  | 100     | 2017-04-27 | offer_accepted             | Refinance|
| 3  | 101     | 2017-04-22 | prequal_completd_no_offer  | Refinance|
| 4  | 101     | 2017-04-23 | offer_accepted             | Refinance|
| 5  | 101     | 2017-04-25 | offer_accepted             | Personal |
| 6  | 102     | 2017-04-27 | offer_accepted             | InSchool |
| 7  | 107     | 2017-04-27 | prequal_response_received  | Personal |
| 8  | 108     | 2017-04-21 | form_in_progress           | Refinance|
| 9  | 108     | 2017-04-27 | offer_accepted             | Refinance|
| 10 | 108     | 2017-04-27 | prequal_response_received  | InSchool |
| 11 | 100     | 2015-04-21 | prequal_completd_offer     | Refinance|

Write a query to return the user IDs of all users that have created at least one submission of type 'Refinance' and at least one submission of type 'InSchool'.

---

## Solution

To solve this problem, we can use PySpark to filter the dataset by user IDs that have submissions of both types, 'Refinance' and 'InSchool'. The solution is as follows:

### PySpark Code:

```python
import pyspark
from pyspark.sql import functions as F

# Sample data
data = [
    (1, 100, "2017-04-21", "prequal_completd_offer", "Refinance"),
    (2, 100, "2017-04-27", "offer_accepted", "Refinance"),
    (3, 101, "2017-04-22", "prequal_completd_no_offer", "Refinance"),
    (4, 101, "2017-04-23", "offer_accepted", "Refinance"),
    (5, 101, "2017-04-25", "offer_accepted", "Personal"),
    (6, 102, "2017-04-27", "offer_accepted", "InSchool"),
    (7, 107, "2017-04-27", "prequal_response_received", "Personal"),
    (8, 108, "2017-04-21", "form_in_progress", "Refinance"),
    (9, 108, "2017-04-27", "offer_accepted", "Refinance"),
    (10, 108, "2017-04-27", "prequal_response_received", "InSchool"),
    (11, 100, "2015-04-21", "prequal_completd_offer", "Refinance"),
]

# Define schema
columns = ["d", "user_id", "created_at", "status", "type"]

# Create DataFrame
df = spark.createDataFrame(data, columns)

# Filter users with Refinance submissions
refinance_users = df.filter(F.col("type") == "Refinance").select("user_id").distinct()

# Filter users with InSchool submissions
inschool_users = df.filter(F.col("type") == "InSchool").select("user_id").distinct()

# Find common users who have both types of submissions
result = refinance_users.intersect(inschool_users)

# Show result
result.show()
