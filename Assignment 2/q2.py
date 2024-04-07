import sys
from pyspark.sql import SparkSession

# you may add more import if you need to


# don't change this line
hdfs_nn = sys.argv[1]

spark = SparkSession.builder.appName("Assigment 2 Question 2").getOrCreate()
# YOUR CODE GOES BELOW
df = spark.read.option("header", "true").option("inferschema", "true").csv("hdfs://%s:9000/assignment2/part1/input/" % (hdfs_nn))

df = df.filter(df["Price Range"].isNotNull())

#group by Price range and see max and min values of Rating
cities_max = df.groupby(["City", "Price Range"]).max("Rating").alias("Rating")
cities_min = df.groupby(["City", "Price Range"]).min("Rating")
cities = cities_max.union(cities_min).sort("City")
cities = cities.withColumnRenamed("max(Rating)", "Rating")

output = df.join(cities, ((cities["City"] == df["City"])& (cities["Rating"] == df["Rating"]) & (cities["Price Range"] == df["Price Range"])), "inner")

#if need drop dups:
output = output.dropDuplicates(subset=["City", "Price Range", "Rating"])

output.show(1000)

output.write.option("header", "true").csv("hdfs://%s:9000/assignment2/output/question2" % (hdfs_nn))
