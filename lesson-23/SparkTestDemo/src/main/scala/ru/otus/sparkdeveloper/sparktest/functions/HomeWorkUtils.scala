package ru.otus.sparkdeveloper.sparktest.functions

import org.apache.spark.sql.{DataFrame, functions}
import org.apache.spark.sql.functions.{
  col,
  collect_list,
  count,
  explode,
  json_object_keys,
  lit,
  to_json
}

object HomeWorkUtils {

  def getCountry(df: DataFrame): DataFrame = {
    val df1 = df
      .select(col("cca3"), col("borders"))
      .filter(functions.size(col("borders")) >= 5)
      .withColumn("id", explode(col("borders")))
      .drop("borders")

    val df2 =
      df.select(col("cca3").as("id"), col("name.official").as("Country"))

    val df3 = df1.join(df2, "id", "LEFT")

    val df4 = df3
      .groupBy(col("cca3"))
      .agg(collect_list("Country").as("BorderCountries"))
      .withColumn("NumBorders", lit(functions.size(col("BorderCountries"))))

    df2
      .join(df4, df2.col("id") === df4.col("cca3"), "Right")
      .drop(col("id"))
      .drop(col("cca3"))
      .sort(col("NumBorders").desc)
  }

  def getLanguage(df: DataFrame): DataFrame = {
    val df1 = df.select(col("languages"), col("name.official"))
    val df2 = df1.withColumn(
      "Languages",
      explode(json_object_keys(to_json(col("languages"))))
    )
    df2
      .groupBy("Languages")
      .agg(
        count("official").as("NumCountries"),
        collect_list("official").as("Countries")
      )
      .orderBy(col("NumCountries").desc)
  }
}
