import os
import shutil
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType, ArrayType
from pyspark.sql.functions import to_date, year, udf, explode, col

if __name__ == "__main__":
    conf = SparkConf().setAppName("bd").setMaster("local[*]")
    sc = SparkContext(conf=conf)
    in_file_path = f"{os.getcwd()}/data_sources/projects_with_employees.csv"
    out_file_path = f"{os.getcwd()}/results/customer_distribution.csv"

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
        year(to_date("PROJECT_START_DATE", "dd-MM-yy")).alias('START_YEAR'),
        year(to_date("PROJECT_END_DATE", "dd-MM-yy")).alias('END_YEAR'),
    )
    df2 = df1.filter(col('PROJECT_STATE') != 'On Hold')

    make_years_array = udf(lambda start, end: list(range(start, end + 1)), ArrayType(IntegerType()))
    df3 = df2.withColumn("YEAR", explode(make_years_array("START_YEAR", "END_YEAR")))
    df3.show()
    df4 = df3.filter(
        (col('YEAR') <= 2018) & (col('CUSTOMER_COUNTRY').isNotNull())
    ).groupBy('CUSTOMER_COUNTRY').pivot('YEAR').count().na.fill(0).orderBy('CUSTOMER_COUNTRY')
    df4.show(500)
    shutil.rmtree(out_file_path, ignore_errors=True)
    df4.coalesce(1).write.csv(out_file_path, header=True)
