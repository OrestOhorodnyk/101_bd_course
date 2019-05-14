import TestDataGenerator.{getDFSample, getExpectedResult}
import org.apache.spark.sql.SparkSession
import org.scalatest.FunSuite
import MainApp.GetMostPopularCountry

class TestGetPopularHotels extends FunSuite{

  test("Test GetNotBookedHotels") {
    val spark = SparkSession.builder.master("local[*]").appName("popular_countries").getOrCreate()

    val sampleDF = getDFSample(spark)
    sampleDF.show()
    val expectedResult = getExpectedResult(spark)
    val result = GetMostPopularCountry(sampleDF)
    result.show()
    assert(expectedResult.collectAsList() == result.collectAsList())
  }

}

