import TestDataGenerator.{getDFSample, getExpectedResult}
import org.apache.spark.sql.SparkSession
import org.scalatest.FunSuite
import MainApp.GetPopularHotels

class TestGetPopularHotels extends FunSuite{

  test("Test GetPopularHotels") {
    val spark = SparkSession.builder.master("local[*]").appName("popular_countries").getOrCreate()

    val sampleDF = getDFSample(spark)
    val expectedResult = getExpectedResult(spark)
    val result = GetPopularHotels(sampleDF)

    assert(expectedResult.collectAsList() == result.collectAsList())
  }

}

