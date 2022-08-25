from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.master("local[*]").appName("testing").getOrCreate()

host="jdbc:mysql://myuser1999.cprdyd3cbdjg.ap-south-1.rds.amazonaws.com:3306/mysqldb?useSSL=false"

uname = "myuser"
password = "password"

## for reading single table
# df= spark.read.format("jdbc").option("url",host).option("user",uname).option("password",password)\
#     .option("dbtable","(select * from asl) t").option("driver","com.mysql.jdbc.Driver").load()
# res = df.na.fill(0,['name'])
#
# res.show()

## for reading Multiple tables
tabs = ['asl','emp','dept']

for i in tabs:
    df= spark.read.format("jdbc").option("url",host).option("user",uname).option("password",password)\
        .option("dbtable",i).option("driver","com.mysql.jdbc.Driver").load()
    df.show()