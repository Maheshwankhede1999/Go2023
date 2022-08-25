from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate()

# data = [1,2,3,4,5,6,7]
#
# nrdd = spark.sparkContext.parallelize(data)
#
# pro = nrdd.map(lambda x:x*100).filter(lambda x:x>30)
#
# for i in pro.collect():
#     print(i)

data = "C:\\bigdata\\drivers\\asl.csv"
ardd = spark.sparkContext.textFile(data)

pro= ardd.filter(lambda x: 'hyd' in x)
for i in pro.collect():
    print(i)
