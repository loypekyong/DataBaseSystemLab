import sys
from pyspark.sql import SparkSession

# you may add more import if you need to


# don't change this line
hdfs_nn = sys.argv[1]

spark = SparkSession.builder.appName("Assigment 2 Question 4").getOrCreate()
# YOUR CODE GOES BELOW

df = spark.read.option("header", "true").option("inferSchema", "true").csv("hdfs://%s:9000/assignment2/part1/input/" % (hdfs_nn))

#counts the number of restaurants by city and cuisine style.
df = df.groupBy("City", "Cuisine Style").count()

df.show()

df.write.option("header", "true").csv("hdfs://%s:9000/sassignment2/output/question4" % (hdfs_nn))

