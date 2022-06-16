import glob
from pyspark.sql import SparkSession

spark=SparkSession.builder.master("local[*]").appName("Survey").getOrCreate()
allfiles=glob.glob(r"D:\3_PySpark\SCD project\Survey_Source\*.csv")
print(allfiles)

rdd1= spark.sparkContext.parallelize(allfiles).map(lambda x: x.split(","))
print(rdd1.collect())