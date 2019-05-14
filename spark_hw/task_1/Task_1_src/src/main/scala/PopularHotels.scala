import org.apache.spark.sql.SparkSession
import java.nio.file.Paths
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, desc}



object MainApp {

  def GetPopularHotels(df: DataFrame): DataFrame ={

    val df1 = df.filter(col("srch_adults_cnt") === 2)

    df1.groupBy(
      "hotel_continent",
      "hotel_country",
      "hotel_market"
    ).count().sort(desc("count")).limit(3)
  }


  def main(args: Array[String]) {
    //  path to input file
    val file_in_path = Paths.get("test.csv.gz").toAbsolutePath.toString
    //  spark app
    val spark = SparkSession.builder.master("local[*]").appName("popularHotels").getOrCreate()
    // load data
    val df = spark.read.format(
      "csv"
    ).option(
      "header", "true"
    ).load(file_in_path)
    df.show()
    val df2 = GetPopularHotels(df)


    df2.show()

  }
}
