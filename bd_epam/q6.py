import os
import shutil
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.types import DoubleType
from pyspark.sql.functions import col, round, avg

if __name__ == "__main__":
    conf = SparkConf().setAppName("bd").setMaster("local[*]")
    sc = SparkContext(conf=conf)
    in_file_path = f"{os.getcwd()}/data_sources/projects_with_employees.csv"
    out_file_path = f"{os.getcwd()}/results/team_size.csv"

    data = sc.textFile(in_file_path)

    spark = SparkSession.builder.master("local[*]").appName("Datacleaning").getOrCreate()
    bd_employees = spark.read.format(
        "csv"
    ).option(
        "header", "true"
    ).option(
        "mode", "DROPMALFORMED"
    ).load(in_file_path)

    for i in bd_employees.schema.fields:
        print(i)

    df1 = bd_employees.select(
        "PROJECT_ID",
        "PROJECT",
        "CUSTOMER_COUNTRY",
        "PROJECT_STATE",
        "CITY",
        "MEDIAN",
        "PROJECT_INDUSTRY"
    )
    df2 = df1.filter(
        col('PROJECT_STATE') == 'Closed'
    ).withColumn(
        "MEDIAN",
        df1.MEDIAN.cast(DoubleType())
    )

    df2.show()
    for i in df2.schema.fields:
        print(i)

    df3 = df2.groupBy(
        'PROJECT_INDUSTRY'
    ).agg(
        round(avg((col('MEDIAN'))), 2).alias('Average team size')
    ).orderBy('Average team size', ascending=False)

    df3.show(500)

    df4 = df2.groupBy(
        'CITY'
    ).agg(
        round(avg((col('MEDIAN'))), 2).alias('Average team size ')
    ).orderBy('Average team size ', ascending=False)
    df4.show(500)
    df5 = df2.agg(round(avg((col('MEDIAN'))), 2).alias('Average team size '))
    shutil.rmtree(out_file_path, ignore_errors=True)
    df5.coalesce(1).write.csv(out_file_path, header=True)
    df3.coalesce(1).write.csv(out_file_path, header=True,  mode="append")
    df4.coalesce(1).write.csv(out_file_path, header=True,  mode="append")
#
