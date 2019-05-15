import os
import shutil
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType, FloatType
from pyspark.sql.functions import to_date, year, sum, when, col, lit, explode

if __name__ == "__main__":
    conf = SparkConf().setAppName("bd").setMaster("local[*]")
    sc = SparkContext(conf=conf)
    in_file_path = f"{os.getcwd()}/data_sources/BDCC_projects_20181008.csv"
    out_file_path = f"{os.getcwd()}/results/total_per_country.csv"

    data = sc.textFile(in_file_path)

    spark = SparkSession.builder.master("local[*]").appName("Datacleaning").getOrCreate()

    df_bdcc_projects = spark.read.format(
        "csv"
    ).option(
        "header", "true"
    ).option(
        "mode", "DROPMALFORMED"
    ).option(
        "dateFormat", "dd-MM-yyyy"
    ).load(in_file_path)

    df_bdcc_projects = df_bdcc_projects.withColumn(
        "PROJECT_END_DATE",
        to_date(df_bdcc_projects.PROJECT_END_DATE, 'dd-MM-yyyy')
    ).withColumn(
        "PROJECT_START_DATE",
        to_date(df_bdcc_projects.PROJECT_START_DATE, 'dd-MM-yyyy')
    ).withColumn(
        'MAX_EMPLOYEES_ON_PROJECT',
        df_bdcc_projects.MAX_EMPLOYEES_ON_PROJECT.cast(IntegerType())
    ).withColumn(
        "PROJECT_DURATION_IN_YEARS",
        df_bdcc_projects.PROJECT_DURATION_IN_YEARS.cast(FloatType())
    ).withColumn(
        "MEDIAN",
        df_bdcc_projects.MEDIAN.cast(FloatType())
    )
    df_bdcc_projects = df_bdcc_projects.withColumn(
        "project_start_year",
        year(df_bdcc_projects.PROJECT_START_DATE)
    ).withColumn(
        "project_end_year",
        year(df_bdcc_projects.PROJECT_END_DATE)
    )

    cnt_cond = lambda cond: sum(when(cond, 1).otherwise(0))
    df_projects_by_year = df_bdcc_projects.groupBy('project_start_year').agg(
        cnt_cond(
            (col('project_start_year') >= year(col('PROJECT_START_DATE')))
            & (col('project_start_year') <= year(col('PROJECT_END_DATE')))
            & (col('PROJECT_STATE') == 'Active')
        ).alias('active_projects'),
        cnt_cond(
            (col('project_start_year') >= year(col('PROJECT_START_DATE')))
            & (col('project_start_year') <= year(col('PROJECT_END_DATE')))
            & (col('PROJECT_STATE') == 'Closed')
        ).alias('closed_projects'),
        cnt_cond(
            (col('project_start_year') >= year(col('PROJECT_START_DATE')))
            & (col('project_start_year') <= year(col('PROJECT_END_DATE')))
            & (col('PROJECT_STATE') == 'On Hold')
        ).alias('projects_on_hold'),
    ).orderBy("project_start_year")

    df_projects_by_year.show()

    # 1. Total amount of projects per country. [Yearly grades (active projects in a year)].
    # Distr over project status (project state) exclude on hold

    df_total_per_country = df_bdcc_projects.groupBy('CUSTOMER_COUNTRY', 'project_start_year').agg(
        cnt_cond(
            (col('project_start_year') >= year(col('PROJECT_START_DATE')))
            & (col('project_start_year') <= year(col('PROJECT_END_DATE')))
            & (col('PROJECT_STATE') == 'Active')
        ).alias('active_projects'),
        cnt_cond(
            (col('project_start_year') >= year(col('PROJECT_START_DATE')))
            & (col('project_start_year') <= year(col('PROJECT_END_DATE')))
            & (col('PROJECT_STATE') == 'Closed')
        ).alias('closed_projects'),
        cnt_cond(
            (col('project_start_year') >= year(col('PROJECT_START_DATE')))
            & (col('project_start_year') <= year(col('PROJECT_END_DATE')))
            & (col('PROJECT_STATE') == 'On Hold')
        ).alias('projects_on_hold'),
    ).orderBy("project_start_year")

    df_total_per_country.show(500)
    sum_by_col = df_projects_by_year.select(
        lit('total').alias('project_start_year'), sum('active_projects'), sum('closed_projects'),
        sum('projects_on_hold')
    )
    print(sum_by_col.collect())
    df_with_totals = df_projects_by_year.union(sum_by_col)
    df_with_totals.show()

    sum_by_col = df_projects_by_year.select(
        lit('CUSTOMER_COUNTRY').alias(''),
        lit('total').alias('project_start_year'),
        sum('active_projects'),
        sum('closed_projects'),
        sum('projects_on_hold')
    )

    df_total_per_country_with_totals = df_total_per_country.union(sum_by_col)
    df_total_per_country_with_totals.show(200)

    shutil.rmtree(out_file_path, ignore_errors=True)
    df_total_per_country_with_totals.coalesce(1).write.csv(out_file_path)
    df_bdcc_projects.select('PROJECT_STATE').distinct().show()

    for i in df_bdcc_projects.schema.fields:
        print(i)
