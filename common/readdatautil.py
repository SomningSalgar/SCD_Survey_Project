from pyspark.sql.functions import *


class ReadDataUtils:
    def readSrcData(self,spark,path):
        df=spark.read.csv(path=path,header=True,inferSchema=True)
        return df

    def dateExtract(self,spark,filename):
        df = spark.sparkContext.parallelize(filename).map(lambda x: x.split("-")).toDF()

        ##   annual|enterprise|survey|financial|year|provisional_2022_05_21.csv
        filedate = df.withColumn("current_date",to_date(regexp_extract(df.columns[-1], "\\d{4}_\\d{1,2}_\\d{1,2}", 0), 'yyyy_MM_dd')).select("current_date")
        return filedate

    def writeTargetData(self,df,path):
        df.write.csv(path,header=True,inferSchema=True)
        print("File is successfully saved!!")



