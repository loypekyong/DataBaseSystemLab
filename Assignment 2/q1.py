import sys
from pyspark.sql import SparkSession
# you may add more import if you need to


# don't change this line
hdfs_nn = sys.argv[1]

spark = SparkSession.builder.appName("Assigment 2 Question 1").getOrCreate()
# YOUR CODE GOES BELOW

df = spark.read.option("header", "true").option("inferschema", "true").csv("hdfs://%s:9000/assignment2/part1/input/" % (hdfs_nn))


df = df.filter(((df["Number of Reviews"].isNotNull()) | (df["Rating"] >= 1.0)))

df.show()

df.write.option("header", "true").csv("hdfs://%s:9000/assignment2/output/question1" % (hdfs_nn))
