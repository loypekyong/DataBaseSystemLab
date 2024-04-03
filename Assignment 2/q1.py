import sys
from pyspark.sql import SparkSession
# you may add more import if you need to


# don't change this line
hdfs_nn = sys.argv[1]

spark = SparkSession.builder.appName("Assigment 2 Question 1").getOrCreate()
# YOUR CODE GOES BELOW

df = spark.read.option("header", "true").csv("hdfs://%s:9000/assignment2/part1/input/" % (hdfs_nn))

#convert the column "Number of Reviews" to float
df = df.withColumn("Number of Reviews", df["Number of Reviews"].cast("float"))

df = df.filter(df["Number of Reviews"] == 0.0 or df["Rating"] >= 1.0)

df.show()

df.write.option("header", "true").csv("hdfs://%s:9000/assignment2/output/question1" % (hdfs_nn))
