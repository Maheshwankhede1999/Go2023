from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate()
sc = spark.sparkContext
data = "D:\\AVD\\spark\\datasets\\asl.csv"
rdd = sc.textFile(data)
res = rdd.filter(lambda x: "hyd" in x)
for i in res.collect():
    print(i)
# df = spark.read.format("csv").option("header","true").option("inferSchema","true").load(data)
# ndf = df.select(col("First Name"),col("Date of Birth"),col("Time of Birth"))
# ndf.printSchema()
# ndf.show(5)
