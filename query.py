from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("SABD").getOrCreate()

df = spark.read \
    .option("header", "true").option("comment", "#") .csv(r"C:\Users\Marco Lioi\Desktop\spark-project\out500_combined+header.csv")

df1 = df.select("ID", "SecType", "Date", "Time", col("Close").cast("double"))
df1 = df1.withColumn("Hour", substring(col("Time"), 1, 2).cast("int"))
df1.createOrReplaceTempView("table1")

query = """SELECT Date,Hour, MIN(Close) 
FROM table1 
WHERE SecType = "E" 
AND ID LIKE "%FR" 
GROUP BY Date,Hour
ORDER BY Hour
"""
spark.sql(query).show()

spark.stop()