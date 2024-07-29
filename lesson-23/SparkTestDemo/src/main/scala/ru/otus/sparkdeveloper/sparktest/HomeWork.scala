package ru.otus.sparkdeveloper.sparktest

import org.apache.logging.log4j.scala.Logging
import ru.otus.sparkdeveloper.sparktest.functions.HomeWorkUtils

object HomeWork extends SparkSessionWrapper with Logging {

  def main(args: Array[String]): Unit = {

    val df = spark.read
      .format("csv")
      .load(args.apply(0))

    val result1 = HomeWorkUtils.getCountry(df)
    val result2 = HomeWorkUtils.getLanguage(df)
  }
}
