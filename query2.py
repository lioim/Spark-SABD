from pyspark.sql import *
from pyspark.sql.functions import *


spark = SparkSession.builder.appName("SABD").getOrCreate()

dataset = spark.read \
    .option("header", "true").option("comment", "#") .csv(r"C:\Users\Marco Lioi\Desktop\spark-project\out500_combined+header.csv")

df = dataset.select("ID", "SecType", "Date", "Time", col("Current price").cast("double")).distinct()\
    .orderBy("ID","Date", "Time")

window_spec = Window().orderBy("Time").rangeBetween(-3600, 0)

df_with_price_change = df.withColumn("prev_price", F.lag("Current price").over(window_spec))
df_with_price_change = df_with_price_change.withColumn("price_change", df_with_price_change["Current price"] - df_with_price_change["prev_price"])

df_with_price_change.select("ID", "SecType", "Date", "Time", "Current price", "price_change").show()



#df.createOrReplaceTempView("table")





df.show()
#spark.sql(prova_sql).show()

spark.stop()