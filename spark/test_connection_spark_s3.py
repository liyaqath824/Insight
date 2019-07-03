# Test if I could import data from AWS S3 to Spark

# Packages
from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext, SparkSession

# Import data from AWS S3
#path = "s3a://hello142/reviews_Office_Products_5.json/Office_Products_5.json"
path = "s3a://hello142/reviews_Office_Products_5.json"
conf = SparkConf().setAppName("ReadJson")
sc = SparkContext(conf = conf)
sqlContext = SQLContext(sc)

# Only select column "asin"
df = sqlContext.read.json(path).select("asin")

# Show the data (i.e. only asin)
df.show()
