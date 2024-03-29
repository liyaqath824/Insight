# Test if I could connect Spark with MySQL database
# Before this step, you should download a JDBC driver corresponds with your MySQL/Python versions
# I choose this JDBC version: https://mvnrepository.com/artifact/mysql/mysql-connector-java/8.0.15
# Need to install this JDBC connector to all master & workers node.

# Packages
from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext, SparkSession

# Test the connection between Spark and MySQL
spark = SparkSession.builder.appName('ReadData').getOrCreate()
df = spark.read.format("jdbc").options(
    url='jdbc:mysql://instance1.ca9mgws4l0bv.us-west-2.rds.amazonaws.com:3306/khan',
    driver = 'com.mysql.cj.jdbc.Driver',
    dbtable = '(SELECT * FROM table_name) as table_new_name',
    user = 'Ali',
    password = 'Alikhan824').load()
df.show()

# Finally, run "spark-submit --jars /path/mysql-connector-java-8.0.15.jar python_codes.py"
