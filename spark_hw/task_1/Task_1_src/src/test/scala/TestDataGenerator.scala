import org.apache.spark.sql.functions.desc
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

object TestDataGenerator {
  def getDFSample(sparkSession: SparkSession): DataFrame = {

    val fields = List(
      StructField("date_time", StringType, nullable = false),
      StructField("site_name", StringType, nullable = false),
      StructField("posa_continent", StringType, nullable = false),
      StructField("user_location_country", StringType, nullable = false),
      StructField("user_location_region", StringType, nullable = false),
      StructField("user_location_city", StringType, nullable = false),
      StructField("orig_destination_distance", StringType, nullable = false),
      StructField("user_id", StringType, nullable = false),
      StructField("is_mobile", StringType, nullable = false),
      StructField("is_package", StringType, nullable = false),
      StructField("channel", StringType, nullable = false),
      StructField("srch_ci", StringType, nullable = false),
      StructField("srch_co", StringType, nullable = false),
      StructField("srch_adults_cnt", StringType, nullable = false),
      StructField("srch_children_cnt", IntegerType, nullable = false),
      StructField("srch_rm_cnt", StringType, nullable = false),
      StructField("srch_destination_id", StringType, nullable = false),
      StructField("srch_destination_type_id", StringType, nullable = false),
      StructField("is_booking", StringType, nullable = false),
      StructField("cnt", StringType, nullable = false),
      StructField("hotel_continent", StringType, nullable = false),
      StructField("hotel_country", StringType, nullable = false),
      StructField("hotel_market", StringType, nullable = false),
      StructField("hotel_cluste", StringType, nullable = false)
    )
    val rows = List(
      List("2014-08-11 07:46:59", "2", "3", "66", "348", "48862", "2234.2641", "12", "0", "1", "9", "2014-08-27", "2014-08-31", "2", 1, "1", "8250", "1", "0", "3", "1", "51", "621", "1"),
      List("2014-08-11 07:46:59", "2", "3", "66", "348", "48862", "2234.2641", "12", "0", "1", "9", "2014-08-27", "2014-08-31", "2", 0, "1", "8250", "1", "0", "3", "1", "51", "621", "1"),
      List("2014-08-11 07:46:59", "2", "3", "66", "348", "48862", "2234.2641", "12", "0", "1", "9", "2014-08-27", "2014-08-31", "2", 0, "1", "8250", "1", "0", "3", "1", "51", "621", "1"),
      List("2014-08-11 08:22:12", "2", "3", "66", "348", "48862", "2234.2641", "12", "0", "1", "9", "2014-08-29", "2014-09-02", "2", 2, "1", "8250", "1", "0", "1", "2", "52", "622", "1"),
      List("2014-08-11 08:22:12", "2", "3", "66", "348", "48862", "2234.2641", "12", "0", "1", "9", "2014-08-29", "2014-09-02", "2", 2, "1", "8250", "1", "0", "1", "2", "52", "622", "1"),
      List("2014-08-11 08:24:33", "2", "3", "66", "348", "48862", "2234.2641", "12", "0", "0", "9", "2014-08-29", "2014-09-02", "3", 3, "1", "8250", "1", "0", "1", "3", "53", "623", "1"),
      List("2014-08-11 08:24:33", "2", "3", "66", "348", "48862", "2234.2641", "12", "0", "0", "9", "2014-08-29", "2014-09-02", "3", 3, "1", "8250", "1", "0", "1", "3", "53", "623", "1"),
      List("2014-08-11 08:24:33", "2", "3", "66", "348", "48862", "2234.2641", "12", "0", "0", "9", "2014-08-29", "2014-09-02", "3", 3, "1", "8250", "1", "0", "1", "3", "53", "623", "1"),
      List("2014-08-11 07:46:59", "2", "3", "66", "348", "48862", "2234.2641", "12", "0", "1", "9", "2014-08-27", "2014-08-31", "2", 4, "1", "8250", "1", "0", "3", "4", "54", "624", "1"),
      List("2014-08-11 07:46:59", "2", "3", "66", "348", "48862", "2234.2641", "12", "0", "1", "9", "2014-08-27", "2014-08-31", "2", 4, "1", "8250", "1", "0", "3", "4", "54", "624", "1"),
      List("2014-08-11 07:46:59", "2", "3", "66", "348", "48862", "2234.2641", "12", "0", "1", "9", "2014-08-27", "2014-08-31", "2", 4, "1", "8250", "1", "0", "3", "4", "54", "624", "1"),
      List("2014-08-11 07:46:59", "2", "3", "66", "348", "48862", "2234.2641", "12", "0", "1", "9", "2014-08-27", "2014-08-31", "2", 4, "1", "8250", "1", "0", "3", "4", "54", "624", "1")
    )
    val m_rows = rows.map(x => Row.fromSeq(x))

    val rdd = sparkSession.sparkContext.makeRDD(m_rows)

    sparkSession.createDataFrame(rdd, StructType(fields))
  }

  def getExpectedResult(sparkSession: SparkSession): DataFrame = {
    val fields = List(
      StructField("hotel_continent", StringType, nullable = false),
      StructField("hotel_country", StringType, nullable = false),
      StructField("hotel_market", StringType, nullable = false),
      StructField("count", IntegerType, nullable = false)
    )
    val rows = List(
      List("4", "54", "624", 4),
      List("1", "51", "621", 3),
      List("2", "52", "622", 2),

    )
    val m_rows = rows.map(x => Row.fromSeq(x))

    val rdd = sparkSession.sparkContext.makeRDD(m_rows)

    sparkSession.createDataFrame(rdd, StructType(fields))

  }
}
