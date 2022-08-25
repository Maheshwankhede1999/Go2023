from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate()


## use case: cleaning of data via sparks functions built-in

# data = "D:\\AVD\\spark\\datasets\\us-500.csv"
# df = spark.read.format("csv").option("header","true").option("inferSchema","true").load(data)
# # ndf = df.groupBy(df.state)\
# #     .agg(count("*").alias("cnt"), collect_list(df.first_name)).orderBy(col("cnt").desc())
# # ndf = df.withColumn("age",lit(18))
# # ndf = df.withColumn("fullname", concat_ws(" ",df.first_name,df.last_name))\
# #     .withColumn("phone1",regexp_replace(col("phone1"),"-","").cast(LongType()))\
# #     .drop("city","state").withColumnRenamed("firstname","f_name")
# ndf = df.withColumn("state",when(col("state")=="NY","NewYork").otherwise(col("state")))
# ndf.show()
# ndf.printSchema()


# ### use case: substring function
# ## substring function returns a part of originl index by using the given input position
# ## substring_index function returns part of string by taking delimiter in consideration
#
data = "D:\\AVD\\spark\\datasets\\us-500.csv"
df = spark.read.format("csv").option("header","true").option("inferSchema","true").load(data)
# ndf =df.withColumn("username",substring_index(col("email"),"@",1)).withColumn("domain_names",substring_index(col("email"),"@",-1))
# nd1 = ndf.groupBy(col("domain_names")).count().orderBy(col("count").desc())
df.show()

#
# ### use case: python functions to be used in spark
# data = "D:\\AVD\\spark\\datasets\\us-500.csv"
# df = spark.read.format("csv").option("header","true").option("inferSchema","true").load(data)
#
#
# def fil_state(st):
#     if st=="NY":
#         return "30% OFF"
#     elif st=="CA":
#         return "50% OFF"
#     else:
#         return "RS 500 OFF"
#
# uf = udf(fil_state)
#
# df.createOrReplaceTempView("tab")
# qry =
# #ndf = df.withColumn("offers",uf(col("state")))
# ndf.show()