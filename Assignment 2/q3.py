import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import concat, lit
# you may add more import if you need to


# don't change this line
hdfs_nn = sys.argv[1]

spark = SparkSession.builder.appName("Assigment 2 Question 3").getOrCreate()
# YOUR CODE GOES BELOW
df = spark.read.option("header", "true").option("inferschema", "true").csv("hdfs://%s:9000/assignment2/part1/input/" % (hdfs_nn))

df = df.groupBy("City").avg("Rating").withColumnRenamed("avg(Rating)", "AverageRating")

top_df = df.orderBy("AverageRating", ascending=False).limit(3).withColumn("RatingGroup", lit("Top"))
bot_df = df.orderBy("AverageRating", ascending=True).limit(3).withColumn("RatingGroup", lit("Bot"))
output = top_df.union(bot_df).orderBy("AverageRating", ascending=False)
output.show()

output.write.option("header", "true").csv("hdfs://%s:9000/assignment2/output/question3" % (hdfs_nn))


