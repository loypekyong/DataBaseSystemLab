import sys 
from pyspark.sql import SparkSession
from pyspark.sql.types import ArrayType, StringType, StructField, StructType
from pyspark.sql.functions import col, explode, from_json, count, array, array_sort

# you may add more import if you need to

# don't change this line
hdfs_nn = sys.argv[1]

spark = SparkSession.builder.appName("Assigment 2 Question 5").getOrCreate()
# YOUR CODE GOES BELOW
schema = ArrayType(StructType([StructField("name", StringType(), False)]))
df = spark.read.option("header",True).parquet("hdfs://%s:9000/assignment2/part2/input/" % (hdfs_nn))
df.printSchema() #check

df = df.drop("crew") # not needed
#select name field from the  objects within the cast col
df = df.withColumn(
    "actor1", explode(from_json(col("cast"), schema).getField("name"))
)
df = df.withColumn(
    "actor2", explode(from_json(col("cast"), schema).getField("name"))
)
#select only necessary cols
df = df.select("movie_id", "title", "actor1", "actor2")
#otherwise we get same actor co-casting with themselves
df = df.filter(col("actor1") != col("actor2"))


#new column created with sorted data --> for comparison later
df = df.withColumn("cast_pair", array(col("actor1"), col("actor2"))).withColumn( "cast_pair", array_sort(col("cast_pair")).cast("string"))
#remove duplicates
df = df.dropDuplicates(["movie_id", "title", "cast_pair"]).sort(col("cast_pair").asc())

#count and then check co-cast atleast 2 movies
counts_df = df.groupBy("cast_pair").agg(count("*").alias("count")).filter(col("count") >= 2).sort(col("cast_pair").asc())

output = counts_df.join(df, ["cast_pair"], "inner").drop("cast_pair", "count").orderBy("actor1", "actor2")
output.show(100, truncate=False) #check further down the table for 100 rows
output.write.option("header", True).mode("overwrite").parquet("hdfs://%s:9000/assignment2/output/question5/" % (hdfs_nn))
