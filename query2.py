from pyspark.sql import *
from pyspark.sql.window import Window
from pyspark.sql.functions import *



spark = SparkSession.builder.appName("SABD").getOrCreate()

dataset = spark.read \
    .option("header", "true").option("comment", "#") .csv(r"C:\Users\Marco Lioi\Desktop\spark-project\out500_combined+header.csv")

df = dataset\
    .select("ID", "SecType", "Date", "Time", col("Close").cast("double"))


df = df.withColumn("datetime", concat(col("Date"), lit(" "), col("Time")))

# Cast the concatenated column to a timestamp
df = df.withColumn("DateTime", to_timestamp(concat_ws(" ", col("Date"), col("Time")), "dd-MM-yyyy HH:mm:ss.SSS"))

df.show()

#df.createOrReplaceTempView("table")


#query = "select * from table where Close > 0 or  current_price > 0 or Currency > 0 limit 100"
#spark.sql(query).show()

spark.stop()