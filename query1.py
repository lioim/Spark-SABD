from pyspark.sql import *
from pyspark.sql.functions import *
import time

spark = SparkSession.builder.appName("SABD").getOrCreate()

df = spark.read \
    .option("header", "true").option("comment", "#") .csv(r"C:\Users\Marco Lioi\Desktop\spark-project\out500_combined+header.csv")

df1 = df.select("ID", "SecType", "Date", "Time", col("Last").cast("double"))
df1 = df1.withColumn("Hour", concat(substring(col("Time"), 1, 2), lit(":00")))
df1.createOrReplaceTempView("table1")

query = """SELECT Date,Hour, ID, MIN(Last) as Min, MAX(Last) as Max, AVG(Last) as Mean
FROM table1 
WHERE SecType = "E" 
AND ID LIKE "%FR" 
GROUP BY Date,Hour,ID
having avg(Last) <> 0
ORDER BY Date, Hour, ID
"""

start_time = time.time()
spark.sql(query).show()

print("Execution time is {} seconds".format(time.time() - start_time))

spark.stop()