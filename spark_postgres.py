from pyspark.sql.session import SparkSession

spark = SparkSession \
    .builder \
    .appName('Python Spark Postgresql') \
    .config("spark.jars", "postgresql-42.2.23.jar") \
    .getOrCreate()

jdbcDF = spark.read.format("jdbc"). \
options(
         url='jdbc:postgresql://localhost:5433/cmkl_rnd_fall_2021', # jdbc:postgresql://<host>:<port>/<database>
         dbtable='mock_data_v1',
         user='postgres',
         password='pass1234',
         driver='org.postgresql.Driver').\
load()
jdbcDF.printSchema()

'''
from pyspark import SparkContext, SparkConf
import os
from pyspark.sql.session import SparkSession

spark = SparkSession \
    .builder \
    .appName('Python Spark Postgresql') \
    .config("spark.jars", "./postgresql-42.2.5.jar") \
    .config('spark.driver.extraClassPath', './postgresql-42.2.5.jar') \
    .getOrCreate()


df = spark.read \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://localhost:5432/mockup_sales_data_v1") \
    .option("dbtable", 'mockup_data_v1') \
    .option("user", "postgres") \
    .option("password", "pass1234") \
    .option("driver", "org.postgresql.Driver") \
    .load()

df.printSchema()
'''

'''
from pyspark.sql import SparkSession

# the Spark session should be instantiated as follows
spark = SparkSession \
    .builder \
    .appName("Python Spark SQL basic example") \
    .config("spark.jars", "postgresql-42.2.23.jarpostgresql-42.2.23.jar") \
    .getOrCreate()
    
 # generally we also put `.master()` argument to define the cluster manager
 # it sets the Spark master URL to connect to, such as “local” to run locally, “local[4]” to run locally with 4 cores, or
 # “spark://master:7077” to run on a Spark standalone cluster.
 # http://spark.apache.org/docs/latest/submitting-applications.html#master-urls
    
# Note: JDBC loading and saving can be achieved via either the load/save or jdbc methods


jdbcDF = spark.read.format("jdbc"). \
options(
         url='jdbc:postgresql://localhost:5432/mockup_sales_data_v1', # jdbc:postgresql://<host>:<port>/<database>
         dbtable='mock_data_v1',
         user='postgres',
         password='pass1234',
         driver='org.postgresql.Driver').\
load()

print("SOMETHING HERE")

# df = spark.read.format("jdbc").options(driver="org.postgresql.Driver", url="jdbc:postgresql://localhost:5432/mock_sales_data_v1", dbtable="mock_data_v1",user="postgres", password="pass1234").load()
# df.printSchema()
'''

'''

spark = SparkSession \
    .builder \
    .appName("Python Spark SQL basic example") \
    .config("spark.jars", "file:///C:/Users/kraifirstspark/spark-3.1.2-bin-hadoop2.7/research_and_development/database_drivers/postgresql-42.2.23.jar") \
    .config("spark.driver.extraClassPath", "file:///C:/Users/kraifirstspark/spark-3.1.2-bin-hadoop2.7/research_and_development/database_drivers/postgresql-42.2.23.jar") \
    .getOrCreate()


df = spark.read \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://localhost:5432/mock_sales_data_v1") \
    .option("dbtable", "mock_data_v1") \
    .option("user", "postgres") \
    .option("password", "pass1234") \
    .option("driver", "org.postgresql.Driver") \
    .load()
'''