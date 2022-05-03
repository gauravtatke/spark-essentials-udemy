package part3typesdatasets

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object ComplexTypes extends App {
    val spark = SparkSession.builder()
      .appName("Complex Types")
      .config("spark.master", "local")
      .getOrCreate()

    val moviesDF = spark.read.option("inferSchema", "true").json("src/main/resources/data/movies.json")
    // dates
    val moviesWithReleaseDate = moviesDF.select(
        col("Title"),
        to_date(col("Release_Date"), "d-MMM-yy").as("Actual_Release")
    )

    val movieWithAge = moviesWithReleaseDate
      .withColumn("Today", current_date())
      .withColumn("Right_Now", current_timestamp())
      .withColumn("Movie_Age", datediff(col("Today"), col("Actual_Release")) / 365) // date_add, date_sub

    movieWithAge.show()
    movieWithAge.select("*").where(col("Movie_Age").isNull).show

    // structures
    // version 1 - with col operator
    moviesDF
      .select(col("Title"), struct(col("US_Gross"), col("Worldwide_Gross")).as("Profit")) // return [US_Gross, Worldwide_Gross]
      .select(col("Title"), col("Profit").getField("US_Gross").as("US_Profit"))
      .show

    // version 2 - with expression strings
    moviesDF
      .selectExpr("Title", "(US_Gross, Worldwide_Gross) as Profit")
      .selectExpr("Title", "Profit.US_Gross")
      .show

    // array
    val moviesWithWords = moviesDF.select(
        col("Title"),
        split(col("Title"), " |,").as("Title_Words")
    ) // returns array of strings of title, space or , are separators

    moviesWithWords.select(
        col("Title"),
        expr("Title_Words[0]"),
        size(col("Title_Words")),
        array_contains(col("Title_Words"), "Love")
    ).show
}
