from pyspark.sql import *
from pyspark.sql.window import Window
from pyspark.sql.functions import *



spark = SparkSession.builder.appName("SABD").getOrCreate()

dataset = spark.read \
    .option("header", "true").option("comment", "#") .csv(r"C:\Users\Marco Lioi\Desktop\spark-project\out500_combined+header.csv")

df = dataset\
    .select("ID", "SecType", "Date", "Time", col("Close").cast("double"))


df = df.withColumn("Hour", substring(col("Time"), 1, 2))
df = df.withColumn("datetime", concat(col("Date"), lit(" "), col("Time")))

# Cast the concatenated column to a timestamp
df = df.withColumn("DateTime", to_timestamp(concat_ws(" ", col("Date"), col("Time")), "dd-MM-yyyy HH:mm:ss.SSS"))




#df.show()

df.createOrReplaceTempView("table")


query = """
select ID, Date, AVG(price_diff), STDDEV_POP(price_diff)
from (select ID, Date,Hour, Close, LAG(Close,1,0) over (ORDER BY Hour) as prev_price,
Close - LAG(Close,1,0) over (ORDER BY Hour) as price_diff
from table
)
group by ID, Date


"""
spark.sql(query).show()

spark.stop()