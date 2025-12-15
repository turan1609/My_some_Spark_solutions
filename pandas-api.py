from pyspark.sql import SparkSession
import os
os.environ['PYARROW_IGNORE_TIMEZONE'] = '1'
import pyspark.pandas as ps

spark = SparkSession.builder \
    .appName("Pandas API on Spark") \
    .config("spark.sql.ansi.enabled", "false") \
    .config("spark.executorEnv.PYARROW_IGNORE_TIMEZONE", "1") \
    .getOrCreate()

ps_df = ps.DataFrame({
    "id": [1, 2, 3, 4, 5],
    "name": ["Alice", "Bob", "Charlie", "David", "Emma"],
    "age": [25, 30, 35, 40, 45],
    "salary": [50000, 60000, 75000, 80000, 120000]
})

print("Pandas-on-Spark DataFrame:")
print(ps_df)

print("\nAverage Age:", ps_df["age"].mean())

print("\nSummary Statistics:")
print(ps_df.describe())

ps_df["salary_after_increment"] = ps_df["salary"] * 1.1
print("\nDataFrame after Salary Increment:")
print(ps_df)

filtered_ps_df = ps_df[ps_df["age"] > 30]
print("\nFiltered DataFrame (age > 30):")
print(filtered_ps_df)

spark_df = ps_df.to_spark()
print("\nConverted Spark DataFrame:")
spark_df.show()

ps_df_from_spark = ps.DataFrame(spark_df)
print("\nReconverted Pandas-on-Spark DataFrame:")
print(ps_df_from_spark)

spark.stop()
