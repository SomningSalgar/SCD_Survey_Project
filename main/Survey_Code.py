from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql import Window

from common.readdatautil import ReadDataUtils

if __name__ == '__main__':
    spark=SparkSession.builder.master("local[*]").appName("Survey").getOrCreate()
    rdu=ReadDataUtils()

    src_path=r"D:\3_PySpark\SCD project\Survey_Source\annual-enterprise-survey-financial-year-provisional_2022_05_21.csv"

    ## Reading Source data
    Src_df=rdu.readSrcData(spark,src_path)

    ## Extracting Date from filename
    filename=["annual-enterprise-survey-financial-year-provisional_2022_05_21.csv"]
    file_date=rdu.dateExtract(spark=spark,filename=filename)

    ## Creating DF with date column
    joined_df=Src_df.join(file_date)
    # joined_df.show()

    ##Writting data in Target Folder for SCD Type1
    joined_df.coalesce(1).write.option('header','true').csv(r"D:\3_PySpark\SCD project\Survey_Target SCD1\SCD1")
    print("Successfully saved")

    ## Writting data in Target Folder for SCD Type2
    windospec = Window.partitionBy("Industry_code_NZSIOC", "Variable_code").orderBy(col("current_date").desc())
    joined_df2=joined_df.withColumn("to_date", lag("current_date",default="2999-12-31").over(windospec))
    joined_df2.coalesce(1).write.option('header','true').csv("D:/3_PySpark/SCD project/Survey_Target SCD2/SCD2")
    print("Successfully saved")


                                    ##'''For Processing Data'''

    filename = "annual-enterprise-survey-financial-year-provisional_2022_05_25.csv" # Change
    new_src_path=r"D:\3_PySpark\SCD project\Survey_Source\{}".format(filename)  #Change
    new_src_df=rdu.readSrcData(spark,new_src_path)
    file_date=rdu.dateExtract(spark=spark,filename=filename)
    date_joined_df = new_src_df.join(file_date)
    # date_joined_df.show()
    print("New source dataframe created")

                                     ##''' SCD Type1'''

    target_path1 = r"D:/3_PySpark/SCD project/Survey_Target SCD1/SCD1"         ##Constant
    trg_df1 = rdu.readSrcData(spark, target_path1)
    # trg_df1.show()
    print("Successfully created Target file dataframe for type1")

    union_df=date_joined_df.union(trg_df1)
    union_df.show()
    print("Unioned new source and target file with date column")


                         # #'''Latest Data'''

    windospec=Window.partitionBy("Industry_code_NZSIOC","Variable_code").orderBy(col("current_date").desc())
    scd_type1=union_df.withColumn("rnm",row_number().over(windospec)).filter(col("rnm")==1).drop("rnm")
    # scd_type1.show(1000)
    print("Extracted latest records: ",scd_type1.count())

    ## '''Overwrite File for lateset data in Mediator'''
    scd_type1.coalesce(1).write.mode("Overwrite").option("header",True).csv(r"D:\3_PySpark\SCD project\Mediator SCD1\SCD1")  # Constant
    mediator_path=r"D:\3_PySpark\SCD project\Mediator SCD1\SCD1"
    scd1df=rdu.readSrcData(spark,mediator_path)
    scd1df.coalesce(1).write.mode("Overwrite").option("header", True).csv(r"D:\3_PySpark\SCD project\Survey_Target SCD1\SCD1")
    print("Successfully saved latest data in SCD type 1")


                                 ##'''SCD Type 2'''

    windospec = Window.partitionBy("Industry_code_NZSIOC", "Variable_code").orderBy(col("current_date").desc())
    joined_df2=date_joined_df.withColumn("to_date", lag("current_date",default="2999-12-31").over(windospec))
    joined_df2.show()
    target_path = r"D:/3_PySpark/SCD project/Survey_Target SCD2/SCD2"         ##Constant
    trg_src_df2 = rdu.readSrcData(spark, target_path)
    trg_src_df2.show()
    union_df2 = joined_df2.union(trg_src_df2)
    union_df2.show()
                              ##'''History Maintained'''

    windospec = Window.partitionBy("Industry_code_NZSIOC", "Variable_code").orderBy(col("current_date").desc())
    scd_type2 = union_df2.withColumn("to_date", lag("current_date", default="2999-12-31").over(windospec))
    scd_type2.show()

                         # ## '''Overwrite File for History Maintained'''

    scd_type2.write.mode('overwrite').option('inferSchema','true').csv(r"D:\3_PySpark\SCD project\Mediator SCD2\SCD2", header=True)  # Constant
    mediator_path = r"D:\3_PySpark\SCD project\Mediator SCD2\SCD2"
    scd_type2df = rdu.readSrcData(spark, mediator_path)
    scd_type2df.coalesce(1).write.mode("Overwrite").option("header", True).option('inferSchema', 'true').csv(
        r"D:\3_PySpark\SCD project\Survey_Target SCD2\SCD2")
    print("Successfully History maintained in SCD type2")