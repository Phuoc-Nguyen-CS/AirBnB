from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType
from pyspark.sql.functions import col, regexp

#
spark = SparkSession.builder.appName("AirBnBAnalysis").getOrCreate()
df = spark.read.csv("listings.csv.gz", header=True, inferSchema=True)

# Cleaning up ID row
df = df.filter(col('id').rlike('^[0-9]+$'))
df = df.withColumn('id', col('id').try_cast(IntegerType()))
df = df.filter(col('id').isNotNull())
df.show(100)