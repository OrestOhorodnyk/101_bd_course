import org.apache.spark.sql.SparkSession
import java.nio.file.Paths
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, desc}



object MainApp {

  def GetMostPopularCountry (df: DataFrame): DataFrame ={
    val df2 = df.filter(
      col("is_booking") === 1 &&
        col("user_location_country") === col("hotel_country")
    )

    df2.groupBy(
      "hotel_country",
    ).count().sort(desc("count")).limit(1)
  }

  def main(args: Array[String]) {
    val file_in_path = Paths.get("train.csv.gz").toAbsolutePath.toString
    val spark = SparkSession.builder.master("local[*]").appName("popular_countries").getOrCreate()

    val df = spark.read.format(
      "csv"
    ).option(
      "header", "true"
    ).load(file_in_path)

    val df3 = GetMostPopularCountry(df)

    df3.show()
  }
}
