package part2dataframes

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Aggregations extends App {
    val spark = SparkSession.builder()
      .appName("Aggregation and Groupings")
      .config("spark.master", "local")
      .getOrCreate()

    val moviesDF = spark.read.option("inferSchema", "true").json("src/main/resources/data/movies.json")
    // counting
    moviesDF.select(count(col("Major_Genre"))).show() // all the values EXCEPT null
    moviesDF.selectExpr("count(Major_Genre)") // all the values EXCEPT null
    moviesDF.select(count("*")).show() // all the values INCLUDING null

    // count distinct
    moviesDF.select(countDistinct("Major_Genre")).show()

    // groupings
    moviesDF
      .groupBy("Major_Genre")
      .count() // eq to select count(*) from moviesDF group by Majore_Genre

    moviesDF
      .groupBy("Major_Genre")
      .avg("IMDB_Rating")

    moviesDF
      .groupBy("Major_Genre")
      .agg(
          count("*").as("N_Movies"),
          avg("IMDB_Rating").as("Avg_Rating")
      ).orderBy("Avg_Rating").show()

    // exercises
    // 1. Sum of all the profits
    moviesDF.select((col("US_Gross") + col("Worldwide_Gross") - col("Production_Budget"))
      .as("Profit"))
      .select(sum(col("Profit")).as("Total_Profit"))
      .show()

    // 2. count of distinct directors
    moviesDF.select(countDistinct("Director")).show()

    // 3. mean and std deviation of US Gross
    moviesDF.select(mean("US_Gross"), stddev("US_Gross")).show()

    // 4. avg IMDB rating and the total US gross per Director
    moviesDF
      .groupBy("Director")
      .agg(
          avg("IMDB_Rating").as("Avg_IMDB_Rating"),
          sum("US_Gross")
      )
//      .orderBy("Avg_IMDB_Rating")
      .orderBy(col("Avg_IMDB_Rating").desc_nulls_last) // for descending, use col object and desc* methods
      .show()
}
