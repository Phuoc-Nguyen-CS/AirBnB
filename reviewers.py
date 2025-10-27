from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder.appName("AirBnBAnalysis").getOrCreate()
df = spark.read.csv('reviews.csv.gz', header=True, inferSchema=True)

# Drop rows with no comments
df = df.na.drop(subset=["comments"])
# Drop rows with non-english comments
df = df.filter(col("comments").rlike(r'^[\x00-\x7F]*$'))

