from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.master("local[*]").appName("test").config("spark.sql.session.timeZone","EST").getOrCreate()

## use case:
#
# data = "D:\\AVD\\spark\\datasets\\10000Records.csv"
# df = spark.read.format("csv").option("header","true").option("inferSchema","true").load(data)
# ndf = df.select(col("First Name"),col("Date of Birth"),col("Time of Birth"))
# ndf.printSchema()
# ndf.show(5)



data = "D:\\AVD\\spark\\datasets\\donations.csv"
df = spark.read.format("csv").option("header","true").option("inferSchema","true").load(data)
# ndf = df.select(col("First Name"),col("Date of Birth"),col("Time of Birth"))
# ndf = df.withColumn(col("dt"),regexp_replace(col("dt"),"/","-"))
ndf = df.withColumn("dt",to_date(col("dt"),"d-M-yyyy")).withColumn("cur_dt",current_date()).withColumn("cur_ts",current_timestamp())\
        .withColumn("dt_diff",datediff(col("cur_ts"),col("dt")))\
        .withColumn("dt_add",date_add(col("dt"),100)) \
        .withColumn("nxt_day", next_day(col("dt"), "Monday"))\
        .withColumn("dt1",date_format(col("dt"),"yyyy-M-d-E"))\
        .withColumn("dayofmonth",dayofmonth(col("dt")))

        # ndf1 = ndf.withColumn("dt_add",date_format(col("dt_add"),"dd/MMMM/yyyy/"))
ndf.show(5,truncate=False)
ndf.printSchema()
df.describe().show()

