from pyspark import SparkContext, SparkConf, SQLContext
import os

conf = (
    SparkConf()
    .setAppName("Spark minIO Test")
    .set("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.2.0")
    .set('spark.driver.extraClassPath', '/Users/kraichamnivikaipong/Documents/cmkl_R&D/fall_2021/database_drivers/aws-java-sdk-bundle-1.12.47.jar') \
    .set("spark.fs.s3a.endpoint", "http://192.168.2.39:9000")
    .set("spark.fs.s3a.access.key", 'kraikrai')
    .set("spark.fs.s3a.secret.key", 'kraikrai')
    .set("spark.fs.s3a.path.style.access", True)
    .set("spark.fs.s3a.connection.ssl.enabled", False)
    .set("spark.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
)
sc = SparkContext(conf=conf).getOrCreate()
sqlContext = SQLContext(sc)

#  .set("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.2.0")
# sc._jsc.hadoopConfiguration().set("fs.s3a.access.key", "access_key")
# sc._jsc.hadoopConfiguration().set("fs.s3a.secret.key", "secret_key")
# sc._jsc.hadoopConfiguration().set("fs.s3a.proxy.host", "minio")
# sc._jsc.hadoopConfiguration().set("fs.s3a.endpoint", "minio")
# sc._jsc.hadoopConfiguration().set("fs.s3a.proxy.port", "9000")
# sc._jsc.hadoopConfiguration().set("fs.s3a.path.style.access", "true")
# sc._jsc.hadoopConfiguration().set("fs.s3a.connection.ssl.enabled", "false")
# sc._jsc.hadoopConfiguration().set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")

print(sc.wholeTextFiles('s3a://datalake/test.txt').collect())
# Returns: [('s3a://datalake/test.txt', 'Some text\nfor testing\n')]
#path = "s3a://user-jitsejan/mario-colors-two/"
#rdd = sc.parallelize([('Mario', 'Red'), ('Luigi', 'Green'), ('Princess', 'Pink')])
#rdd.toDF(['name', 'color']).write.csv(path)