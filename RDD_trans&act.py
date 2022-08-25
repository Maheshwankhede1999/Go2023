from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate()


# data = "C:\\bigdata\\drivers\\asl.csv"
# sc = spark.sparkContext
# mrdd = sc.textFile(data)
# # res = mrdd.map(lambda x:x.split(",")).filter(lambda x:'hyd' in x[2])
# res = mrdd.map(lambda x:x.split(",")).filter(lambda x:x[2]=='hyd')
# for i in res.collect():
#         print(i)

data = "C:\\bigdata\\drivers\\asl.csv"
sc = spark.sparkContext
mrdd = sc.textFile(data)
# res = mrdd.map(lambda x:x.split(",")).filter(lambda x:'hyd' in x[2])
res = mrdd.filter(lambda x: 'age' not in x).map(lambda x:x.split(",")).filter(lambda x:int(x[1])>30)
for i in res.collect():
        print(i)