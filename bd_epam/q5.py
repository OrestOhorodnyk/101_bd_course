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
    out_file_path = f"{os.getcwd()}/results/project_duration.csv"

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
        "PROJECT_DURATION_IN_YEARS",
    )
    df2 = df1.filter(
        col('PROJECT_STATE') == 'Closed'
    ).withColumn(
        "PROJECT_DURATION_IN_YEARS",
        df1.PROJECT_DURATION_IN_YEARS.cast(DoubleType())
    )

    df2.show()
    for i in df2.schema.fields:
        print(i)

    df3 = df2.filter(
        col('CUSTOMER_COUNTRY').isNotNull()
    ).groupBy(
        'CUSTOMER_COUNTRY'
    ).agg(
        round(avg((col('PROJECT_DURATION_IN_YEARS'))), 2).alias('Average project duration (years)')
    ).orderBy('Average project duration (years)', ascending=False)

    df3.show(500)

    df4 = df2.groupBy(
        'CITY'
    ).agg(
        round(avg((col('PROJECT_DURATION_IN_YEARS'))), 2).alias('Average project duration (years)')
    ).orderBy('Average project duration (years)', ascending=False)
    df4.show(500)
    shutil.rmtree(out_file_path, ignore_errors=True)
    df3.coalesce(1).write.csv(out_file_path, header=True)
    df4.coalesce(1).write.csv(out_file_path, header=True,  mode="append")
#