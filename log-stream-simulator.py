import random
import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf
from pyspark.sql.types import StringType
from pyspark.sql.functions import regexp_extract

LOG_FILE = "./access_log.txt"

spark = SparkSession.builder \
    .appName("SparkStreamingLogSimulator") \
    .getOrCreate()

@udf(StringType())
def get_random_log_line():
    try:
        if not os.path.exists(LOG_FILE):
            return None

        file_size = os.path.getsize(LOG_FILE)
        if file_size == 0:
            return None

        with open(LOG_FILE, "r") as lf:
            while True:
                random_position = random.randint(0, file_size - 1)
                lf.seek(random_position)
                lf.readline()
                line = lf.readline().strip()
                
                if line:
                    return line

    except Exception as e:
        print(str(e))
        return None
    
rate_df = spark.readStream \
    .format("rate") \
    .option("rowsPerSecond", 5) .load()
    
accessLines = rate_df \
    .withColumn("value", get_random_log_line())
    
contentSizeExp = r'\s(\d+)$'
statusExp = r'\s(\d{3})\s'
generalExp = r'\"(\S+)\s(\S+)\s*(\S*)\"'
timeExp = r'\[(\d{2}/\w{3}/\d{4}:\d{2}:\d{2}:\d{2} -\d{4})]'
hostExp = r'(^\S+\.[\S+\.]+\S+)\s'

logsDF = accessLines.select(regexp_extract('value', hostExp, 1).alias('host'),
                         regexp_extract('value', timeExp, 1).alias('timestamp'),
                         regexp_extract('value', generalExp, 1).alias('method'),
                         regexp_extract('value', generalExp, 2).alias('endpoint'),
                         regexp_extract('value', generalExp, 3).alias('protocol'),
                         regexp_extract('value', statusExp, 1).cast('integer').alias('status'),
                         regexp_extract('value', contentSizeExp, 1).cast('integer').alias('content_size'))

statusCountsDF = logsDF.groupBy(logsDF.status).count()

query = ( statusCountsDF.writeStream.outputMode("complete").format("console").queryName("counts").start() )

query.awaitTermination()

spark.stop()
