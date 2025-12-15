from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, IntegerType, FloatType

spark = SparkSession.builder.appName("CustomersDatasetsWithDataFrame").getOrCreate()

schema = StructType([ \
    StructField("customer_id", IntegerType(), False),\
    StructField("product_id", IntegerType(), False),\
    StructField("payment", FloatType(), False)])

df = spark.read.schema(schema).csv("file:///home/turan/SparkCourse/customer-orders.csv")

selected_df = df.select("customer_id", "payment")

summed_df = selected_df.groupBy("customer_id").agg(
        func.round(func.avg("payment"), 2).alias("average_spent")
)

sortedAverages = summed_df.sort(func.col("average_spent").desc())

sortedAverages.show(sortedAverages.count())

spark.stop()
