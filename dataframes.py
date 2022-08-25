from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate()

# ## how to create dataframe
# data = "D:\\AVD\\spark\\datasets\\donations1.csv"
# df = spark.read.format("csv").option("header","true").load(data)
# df.printSchema()
# df.show()

# ### use case: when you have to skip malicious first row and then header
# data = "D:\\AVD\\spark\\datasets\\donations1.csv"
# #  created rdd to cean the data
# rdd = spark.sparkContext.textFile(data)
# # skip first line
# skip=rdd.first()
# res = rdd.filter(lambda x:x!=skip)
# # dataframe created
# df = spark.read.csv(res,header=True,inferSchema=True)
# df.printSchema()
# df.show(5)

### use case: bank file which has ";" as a delimiter between fields
#
# data = "D:\\AVD\\spark\\datasets\\bank-full.csv"
# df = spark.read.format("csv").option("header","true").option("inferSchema","true").option("sep",";").load(data)
# # "sep" is used in option to use ; as a delimiter
# # df.show()
# # df.printSchema()
#
# ## data processing
# res = df.select(col("age"), col("job")).where((col("age")>60) | (col("job")=="entreprenuer"))
# res.show()
# # res.printSchema()

## use case: SQL friendly interface
#
# data = "D:\\AVD\\spark\\datasets\\bank-full.csv"
# df = spark.read.format("csv").option("header","true").option("sep",";").option("inferSchema","true").load(data)
#
#
# df.createOrReplaceTempView("tab")
#
# res = spark.sql("select * from tab where age<60 and age>30")
# res.show(5)

## use case: programming friendly interface
# data = "D:\\AVD\\spark\\datasets\\10000Records.csv"
# df = spark.read.format("csv").option("header","true").option("inferSchema","true").load(data)
# res = df.groupby(col("Gender")).count()
# df.show(5,truncate=False)

## use case: cleaning the columns(schema)
# data = "D:\\AVD\\spark\\datasets\\10000Records.csv"
# df = spark.read.format("csv").option("header","true").option("inferSchema","true").load(data)
# import re
# num = int(df.count())
# cols = [re.sub('[^a-zA-Z0-9]',"",c.lower()) for c in df.columns]
# ndf = df.toDF(*cols)
# ndf.printSchema()
# ndf.show(5,truncate=False)
