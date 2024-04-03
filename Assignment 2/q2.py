import sys
from pyspark.sql import SparkSession

# you may add more import if you need to


# don't change this line
hdfs_nn = sys.argv[1]

spark = SparkSession.builder.appName("Assigment 2 Question 2").getOrCreate()
# YOUR CODE GOES BELOW
df = spark.read.option("header", "true").csv("hdfs://%s:9000/assignment2/part1/input/" % (hdfs_nn))

#ignore rows with null values in the column "Price Range"
df = df.filter(df["Price Range"].isNotNull())

#convert the column "Rating" to float
df = df.withColumn("Rating", df["Rating"].cast("float"))

#group by Price range and see max and min values of Rating
df = df.grouby("Price Range").agg({"Rating": "max", "Rating": "min"})

df.show()

df.write.option("header", "true").csv("hdfs://%s:9000/assignment2/output/question2" % (hdfs_nn))
