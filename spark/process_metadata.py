# Write product metadata (11 GB) from Spark to MySQL

# Import packages
import re
from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext, functions
from pyspark.sql.types import FloatType, ArrayType, StringType, IntegerType
from textblob import TextBlob
import json
# Get metadata from AWS S3
#path = "s3a://hello142/reviews_Office_Products_5.json/Office_Products_5.json"
path = "s3a://hello142/reviews_Office_Products_5.json"
conf = SparkConf().setAppName("ReadJson")
sc = SparkContext(conf = conf)
sqlContext = SQLContext(sc)

# Read a json file
data = sqlContext.read.format('json').\
            options(header='true', inferSchema='true').\
            load(path)
data = data.withColumn("category", data.categories[0][0])

# Select certain columns we want
results = data.select("asin", "title", "price", "category", "imUrl")


# Write the metadata to MySQL
results.write.format("jdbc").options(
    url='jdbc:mysql://instance1.ca9mgws4l0bv.us-west-2.rds.amazonaws.com:3306/instance1',
    driver = 'com.mysql.cj.jdbc.Driver',
    dbtable = 'table_name',
    user = 'Ali',
    password = 'Alikhan824').mode('append').saveAstable('Bucketed_table')
