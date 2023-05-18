# importing pyspark
from pyspark.sql import SparkSession

# start pyspark session
spark = SparkSession.builder \
    .appName("MyApp") \
    .getOrCreate()

# read csv
# loading into dataframe
df = spark.read.format("csv").option("header", "true").load("file.csv")

# display dataframe schema
df.printSchema()

# display rows of the dataframe
df.show()


