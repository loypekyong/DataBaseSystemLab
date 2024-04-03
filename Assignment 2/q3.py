import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import concat, lit
# you may add more import if you need to


# don't change this line
hdfs_nn = sys.argv[1]

spark = SparkSession.builder.appName("Assigment 2 Question 3").getOrCreate()
# YOUR CODE GOES BELOW
df = spark.read.option("header", "true").csv("hdfs://%s:9000/assignment2/part1/input/" % (hdfs_nn))

#convert the column "Rating" to float
df = df.withColumn("Rating", df["Rating"].cast("float"))

df = df.groupBy("City").avg("Rating")

# extracts the three cities with the highest and lowest average rating per restaurant

df = df.orderBy("avg(Rating)", ascending=False).limit(3).union(df.orderBy("avg(Rating)", ascending=True).limit(3))

RatingGroup = ["Top", "Top", "Top", "Bottom", "Bottom", "Bottom"]

ratingdf = spark.createDataFrame(RatingGroup, "string").toDF("RatingGroup")

df = df.select["City", "avg(Rating)"].alias("City", "AverageRating")

df = df.withColumn("RatingGroup", ratingdf["RatingGroup"])

df.show()

df.write.option("header", "true").csv("hdfs://%s:9000/assignment2/output/question3" % (hdfs_nn))


