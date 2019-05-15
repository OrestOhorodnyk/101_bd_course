import os
import shutil
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.types import DoubleType
from pyspark.sql.functions import col, round, avg, count

if __name__ == "__main__":
    conf = SparkConf().setAppName("bd").setMaster("local[*]")
    sc = SparkContext(conf=conf)
    in_file_path = f"{os.getcwd()}/data_sources/projects_with_employees.csv"
    out_file_path = f"{os.getcwd()}/results/projects_by_indastry.csv"

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

    df1 = bd_employees.groupBy(
        'PROJECT_INDUSTRY'
    ).agg(count(col('PROJECT_ID')).alias('Projects')).orderBy('Projects', ascending=False)

    df1.show()

    shutil.rmtree(out_file_path, ignore_errors=True)
    df1.coalesce(1).write.csv(out_file_path, header=True)
#
