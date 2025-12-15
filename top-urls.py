from pyspark.sql import SparkSession

import pyspark.sql.functions as func

spark = SparkSession.builder.appName("StructuredStreaming").getOrCreate()

accessLines = spark.readStream.text("logs")

contentSizeExp = r'\s(\d+)$'
statusExp = r'\s(\d{3})\s'
generalExp = r'\"(\S+)\s(\S+)\s*(\S*)\"'
timeExp = r'\[(\d{2}/\w{3}/\d{4}:\d{2}:\d{2}:\d{2} -\d{4})]'
hostExp = r'(^\S+\.[\S+\.]+\S+)\s'

logsDF = accessLines.select(func.regexp_extract('value', hostExp, 1).alias('host'),
                         func.regexp_extract('value', timeExp, 1).alias('timestamp'),
                         func.regexp_extract('value', generalExp, 1).alias('method'),
                         func.regexp_extract('value', generalExp, 2).alias('endpoint'),
                         func.regexp_extract('value', generalExp, 3).alias('protocol'),
                         func.regexp_extract('value', statusExp, 1).cast('integer').alias('status'),
                         func.regexp_extract('value', contentSizeExp, 1).cast('integer').alias('content_size'))

logsDF2 = logsDF.withColumn("eventTime", func.current_timestamp())

endpointCounts = logsDF2.groupBy(func.window(func.col("eventTime"), \
      "30 seconds", "10 seconds"), func.col("endpoint")).count()

sortedEndpointCounts = endpointCounts.orderBy(func.col("count").desc())

query = sortedEndpointCounts.writeStream.outputMode("complete").format("console") \
      .queryName("counts").start()

query.awaitTermination()

spark.stop()

