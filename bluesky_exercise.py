import random
import os
import json
import re
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, udtf
from pyspark.sql.types import StringType

LOG_FILE = "/home/turan/SparkCourse/bluesky.jsonl"

spark = SparkSession.builder \
    .appName("BlueSkyHashtags") \
    .config("spark.sql.execution.pythonUDTF.enabled", "true") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

@udf(StringType())
def get_random_tweet():
    try:
        file_size = os.path.getsize(LOG_FILE)
        with open(LOG_FILE, "r") as f:
            
            f.seek(random.randint(0, file_size - 200)) 
            f.readline()
            line = f.readline()
            return line.strip() if line else None
    except:
        return None


@udtf(returnType="hashtag: string")
class HashtagExtractor:
    def eval(self, json_str: str):
        if json_str:
            try:
                
                data = json.loads(json_str)
                text = data.get("text", "")
                
                hashtags = re.findall(r"#\w+", text)
                
                for tag in hashtags:
                    yield (tag.lower(),)
            except:
                pass

spark.udtf.register("extract_hashtags", HashtagExtractor)

rate_df = spark.readStream.format("rate").option("rowsPerSecond", 10).load()

raw_tweets = rate_df.withColumn("value", get_random_tweet()).filter("value IS NOT NULL")

raw_tweets.createOrReplaceTempView("stream_data")

hashtag_counts = spark.sql("""
    SELECT hashtag, COUNT(*) as count
    FROM stream_data, LATERAL extract_hashtags(value)
    GROUP BY hashtag
    ORDER BY count DESC
    LIMIT 10
""")

query = hashtag_counts.writeStream \
    .outputMode("complete") \
    .format("console") \
    .start()

query.awaitTermination()
spark.stop()
