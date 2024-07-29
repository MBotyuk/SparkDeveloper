package ru.otus.sparkdeveloper.sparktest.functions

import com.github.mrpowers.spark.fast.tests.DatasetComparer
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col
import org.scalatest.BeforeAndAfterEach
import org.scalatest.funsuite.AnyFunSuite
import ru.otus.sparkdeveloper.sparktest.SparkSessionTestWrapper

class HomeWorkUtilsTest extends AnyFunSuite with BeforeAndAfterEach with SparkSessionTestWrapper with DatasetComparer {

  var testDf: DataFrame = _

  override def beforeEach(): Unit = {
    testDf = spark.read
      .format("json")
      .option("mode", "FAILFAST")
      .option("multiline", value = true)
      .load("src/test/resources/countries.json")
  }

  test("testGetCountry") {
    val resultTest = HomeWorkUtils.getCountry(testDf)

    assert(resultTest.count() == 60)
  }

  test("testGetCountryRating") {
    import spark.implicits._
    val expectedDF = Seq(
      ("People's Republic of China"),
      ("Russian Federation"),
      ("Federative Republic of Brazil"),
      ("Democratic Republic of the Congo"),
      ("Federal Republic of Germany")
    ).toDF("Country")

    val actualDF = HomeWorkUtils.getCountry(testDf).select(col("Country")).limit(5)

    assertSmallDatasetEquality(actualDF, expectedDF)
  }

  test("testGetLanguage") {
    val resultTest = HomeWorkUtils.getLanguage(testDf)

    assert(resultTest.count() == 153)
  }

  test("testGetLanguageRating") {
    import spark.implicits._
    val expectedDF = Seq(
      ("eng"),
      ("fra"),
      ("ara"),
      ("spa"),
      ("por")
    ).toDF("Languages")

    val actualDF = HomeWorkUtils.getLanguage(testDf).select(col("Languages")).limit(5)

    assertSmallDatasetEquality(actualDF, expectedDF)
  }

}
