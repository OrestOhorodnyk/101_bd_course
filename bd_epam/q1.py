import os
import shutil
import time
from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType, ArrayType
from pyspark.sql.functions import to_date, year, col, explode, udf

if __name__ == "__main__":
    conf = SparkConf().setAppName("bd").setMaster("local[*]")
    print(os.getcwd())
    in_file_path = "/home/orest/Documents/bd_epam/data_sources/BDCC_projects_20181008.csv"
    out_file_path = f"{os.getcwd()}/total_per_country.csv"

    spark = SparkSession.builder.master("local[*]").appName("Datacleaning").getOrCreate()

    df_bdcc_projects = spark.read.format(
        "csv"
    ).option(
        "header", "true"
    ).load(in_file_path)
    for i in df_bdcc_projects.schema.fields:
        print(i)

    df_bdcc_projects.show(n=10, truncate=False)
    df2 = df_bdcc_projects.select(
        "PROJECT_ID",
        "PROJECT",
        "CUSTOMER_COUNTRY",
        "PROJECT_STATE",
        year(to_date("PROJECT_START_DATE", "dd-MM-yy")).alias('START_YEAR'),
        year(to_date("PROJECT_END_DATE", "dd-MM-yy")).alias('END_YEAR'),
    )
    # df2.show(n=10, truncate=False)
    print(f'{"*" * 40}Total amount of projects per country{"*" * 40}')
    df3 = df2.groupBy("CUSTOMER_COUNTRY").count().orderBy('CUSTOMER_COUNTRY')

    df3.show(n=10, truncate=False)
    time.sleep(100)
    # makeYearsArray = udf((tr, end: String) = > (start.toInt to end.toInt).toArray)

    make_years_array = udf(lambda start, end: list(range(start, end + 1)), ArrayType(IntegerType()))
    df4 = df2.withColumn("YEAR", explode(make_years_array("START_YEAR", "END_YEAR")))
    # df4.show()

    # df4 = df4.groupBy('CUSTOMER_COUNTRY', 'YEAR').count().alias('PROJECTS').orderBy('YEAR')
    # df4.show()
    df5 = df4.filter((col('YEAR') <= 2018) & (col('PROJECT_STATE') != 'On Hold'))
    print(f'{"*" * 40}Total amount of projects per country yearly grade{"*" * 40}')
    df5 = df5.groupBy('CUSTOMER_COUNTRY').pivot('YEAR').count().orderBy('CUSTOMER_COUNTRY')
    # df4.show(500)
    # df5.show(500)
    df5.na.fill(0).show(500)
    shutil.rmtree(out_file_path, ignore_errors=True)
    df5.coalesce(1).write.csv(out_file_path, header=True)
    print(type(df5))
