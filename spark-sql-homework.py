from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("SparkSql").getOrCreate()

people = spark.read.option("header", "true").option("inferSchema", "true")\
    .csv("file:///home/turan/SparkCourse/fakefriends-header.csv")

ageCounts = people.groupBy("age").count()

avgFriendsByAge = people.groupBy("age").avg("friends")

results = ageCounts.join(avgFriendsByAge, "age")

results.orderBy("age").show()

spark.stop()
