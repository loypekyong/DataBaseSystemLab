import sys
from pyspark.sql import SparkSession

# you may add more import if you need to


# don't change this line
hdfs_nn = sys.argv[1]

spark = SparkSession.builder.appName("Assigment 2 Question 4").getOrCreate()
# YOUR CODE GOES BELOW

df = spark.read.option("header", "true").csv("hdfs://%s:9000/assignment2/part1/input/" % (hdfs_nn))

rdd = df.rdd

# Transform the 'Cuisine Style' column into a list, and create a new row for each element in the list

rdd = rdd.flatMap(lambda row: [(row["City"], cuisine_style, 1) for cuisine_style in row["Cuisine Style"][2:-2]
.replace("]", "").replace("'", "").split(", ")])

# Convert the RDD back to a DataFrame
df = rdd.toDF(["City", "Cuisine Style", "Count"])
# Count the number of restaurants by city and cuisine style
df = df.groupBy("City", "Cuisine Style").sum("Count").orderBy("City")

df.show()

df.write.option("header", "true").csv("hdfs://%s:9000/assignment2/output/question4" % (hdfs_nn))