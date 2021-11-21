from pyspark import SparkContext
from pyspark.sql import SparkSession
from delta import *

builder = SparkSession.builder \
        .appName('Python Spark Minio') \
        .config("spark.jars", "/Users/kraichamnivikaipong/Documents/cmkl_R&D/fall_2021/database_drivers/hadoop-aws-3.2.0.jar") \
        .config("spark.driver.extraClassPath", "/Users/kraichamnivikaipong/Documents/cmkl_R&D/fall_2021/database_drivers/aws-java-sdk-bundle-1.11.375.jar") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") 

spark = spark = configure_spark_with_delta_pip(builder).getOrCreate()

def load_config(spark_context: SparkContext):
    spark_context._jsc.hadoopConfiguration().set('fs.s3a.access.key','kraikrai')
    spark_context._jsc.hadoopConfiguration().set('fs.s3a.secret.key','kraikrai')
    spark_context._jsc.hadoopConfiguration().set('fs.s3a.path.style.access','true')
    spark_context._jsc.hadoopConfiguration().set('fs.s3a.impl','org.apache.hadoop.fs.s3a.S3AFileSystem')
    spark_context._jsc.hadoopConfiguration().set('fs.s3a.endpoint','http://localhost:9000')
    spark_context._jsc.hadoopConfiguration().set('fs.s3a.connection.ssl.enabled','false')



def readCSV(spark):
    df = spark.read.csv('s3a://csv/Supermarket_CustomerMembers')
    df.printSchema()

def writeCSV(spark):
    df = spark.read.csv('/Users/kraichamnivikaipong/Documents/cmkl_R&D/fall_2021/testing/data/csv/Supermarket_CustomerMembers.csv')
    df.write.option("header", True).csv('s3a://csv/Supermarket_CustomerMembers')

def writeDelta(spark):
    df = spark.read.csv('/Users/kraichamnivikaipong/Documents/cmkl_R&D/fall_2021/testing/data/csv/random_glossery.csv')
    df = df.write.format("delta").save("s3a://delta/random_glossery")


load_config(spark.sparkContext)
writeDelta(spark)