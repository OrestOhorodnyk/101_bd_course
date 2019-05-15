import os
import shutil
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession

if __name__ == "__main__":
    conf = SparkConf().setAppName("bd").setMaster("local[*]")
    sc = SparkContext(conf=conf)
    in_file_path = f"{os.getcwd()}/data_sources/projects_with_employees.csv"
    out_file_path = f"{os.getcwd()}/results/industry_by_ofice.csv"

    data = sc.textFile(in_file_path)

    spark = SparkSession.builder.master("local[*]").appName("Datacleaning").getOrCreate()
    df1 = spark.read.format(
        "csv"
    ).option(
        "header", "true"
    ).option(
        "mode", "DROPMALFORMED"
    ).load(in_file_path)

    df2 = df1.groupBy('CITY').pivot('PROJECT_INDUSTRY').count().na.fill(0).orderBy('CITY')
    df2.show(100)
    shutil.rmtree(out_file_path, ignore_errors=True)
    df2.coalesce(1).write.csv(out_file_path, header=True)

    df3 = df1.groupBy('CUSTOMER_COUNTRY').pivot('PROJECT_INDUSTRY').count().na.fill(0).orderBy('CUSTOMER_COUNTRY')
    out_file_path = f"{os.getcwd()}/results/industry_by_customer_country.csv"
    shutil.rmtree(out_file_path, ignore_errors=True)
    df3.coalesce(1).write.csv(out_file_path, header=True)
