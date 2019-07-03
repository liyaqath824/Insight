# Write data to MySQL table
# Notes: I've already created a MySQL table with the same data schema

# Packages
from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext, SparkSession
import configparser
import boto3
config = configparser.ConfigParser()
config.read('ali.cfg')
access_id = config.get('AWS', "access_key") 
access_key = config.get('AWS', "secret_key")
# Load data from AWS S3 to Spark
s3=boto3.resource('s3',aws_access_key_id=access_id, aws_secret_access_key=access_key)
bucket= s3.Bucket('hello142')
path = "s3a://hello142/reviews_Office_Products_5.json/Office_Products_5.json"
conf = SparkConf().setAppName("ReadJson")
sc = SparkContext(conf = conf)

spark = SparkSession.builder.appName("Overwrite").getOrCreate()

sqlContext = SQLContext(sc)
df = sqlContext.read.json(path).select("asin", "overall", "reviewTime", "reviewText")

# Append data from Spark to MySQL table (table name is "reviews")
df.write.format("jdbc").options(
    url='jdbc:mysql://instance1.ca9mgws4l0bv.us-west-2.rds.amazonaws.com:3306/database_name',
    driver = 'com.mysql.cj.jdbc.Driver',
    dbtable = 'table_name',
    user = 'Ali',
    password = 'Alikhan824').mode('append').save()

