import pyspark

from pyspark import SparkConf
from pyspark.sql import SparkSession

# Creating Spark Session
spark = SparkSession \
    .builder \
    .appName("ReadingJson") \
    .getOrCreate()

# Reading jsonFile
path = "data/sparkify_log_small.json"

log_small = spark.read.json(path)
log_small.show(5)

#Saving CSV

out_path = "data/sparkify_log_small.csv"
log_small.write.save(out_path, format='csv', header=True)