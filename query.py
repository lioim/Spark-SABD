from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("SABD").getOrCreate()

df = spark.read \
    .option("header", "true").option("comment", "#") .csv(r"C:\Users\Marco Lioi\Desktop\spark-project\out500_combined+header.csv")

df.printSchema()

spark.stop()