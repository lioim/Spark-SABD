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



df.createOrReplaceTempView("table")


query = """
select ID, Date, AVG(price_diff) as mean, STDDEV_POP(price_diff) as std_dev
from (select ID, Date,Hour, Close, LAG(Close,1,0) over (ORDER BY Hour) as prev_price,
Close - LAG(Close,1,0) over (ORDER BY Hour) as price_diff
from table
)
group by ID, Date


"""
spark.sql(query).createOrReplaceTempView("statistics")

best = """
select *
from (select ID, Date, mean, std_dev, row_number() over (partition by Date order by mean desc) as row_num
from statistics)
where row_num <= 5

"""

worst = """
select *
from (select ID, Date, mean, std_dev, row_number() over (partition by Date order by mean asc) as row_num
from statistics)
where row_num <= 5

"""
spark.sql(best).show()
spark.sql(worst).show()

spark.stop()