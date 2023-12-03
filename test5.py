# ====================================================================================================
# Python Test and Dataset | Question 5 | Mong | Python 3.10.6
# https://docs.google.com/spreadsheets/d/1J5p2t4mOHgdeq-p3g214JLr7wws-06KCA1v8nI2hazs/
# ----------------------------------------------------------------------------------------------------
# Import

import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import expr


# ----------------------------------------------------------------------------------------------------
# Prepare CSV file

df1 = pd.read_excel('Python test and Dataset.xlsx', sheet_name='pricing_project_dataset')
df1.to_csv('test_dataset_q1.csv', encoding='utf-8-sig', index=False)


# ----------------------------------------------------------------------------------------------------
# Run PySpark | Step 1
# - Credit: ChatGPT - How to write PySpark to read the data from the CSV file and create a table

# Create a Spark session
spark = SparkSession.builder.appName("test_question_5_pyspark").getOrCreate()

# Define the path to the CSV file
csv_file_path = 'test_dataset_q1.csv'

# Read data from the CSV file into a DataFrame
df = spark.read.csv(csv_file_path, header=True, inferSchema=True)

# Show the first few rows of the DataFrame
# df.show()

# Create a temporary table for the DataFrame
table_name = 'local_price_competitive_by_sku'
df.createOrReplaceTempView(table_name)

# Now you can use Spark SQL to query the data
# result = spark.sql(f"SELECT * FROM {table_name}")
# result.show()


# ----------------------------------------------------------------------------------------------------
# Run PySpark | Step 2

# Calculate the 70th percentile
percentile_70 = df.approxQuantile('shopee_order', [0.7], 0.1)[0] # Top30% = 131
df_final = df.withColumn("is_top30", expr(f"CASE WHEN shopee_order >= {percentile_70} THEN 1 ELSE 0 END"))

# Show the result
df_final.show()

# Stop the Spark session
spark.stop()

