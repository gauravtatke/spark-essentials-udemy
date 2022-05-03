package part3typesdatasets

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object ManagingNulls extends App {
    val spark = SparkSession.builder()
      .appName("Managing Nulls")
      .config("spark.master", "local")
      .getOrCreate()

    val moviesDF = spark.read.option("inferSchema", "true").json("src/main/resources/data/movies.json")
    // select first non-null value
    moviesDF.select(
        col("Title"),
        col("Rotten_Tomatoes_Rating"),
        col("IMDB_Rating"),
        coalesce(col("Rotten_Tomatoes_Rating"), col("IMDB_Rating") * 10)
    ).show

    // checking for nulls
    moviesDF.select("*").where(col("Rotten_Tomatoes_Rating").isNotNull).show

    // nulls when ordering
    moviesDF.orderBy(col("IMDB_Rating").desc_nulls_first).show

    // removing nulls
    moviesDF.select("Title", "IMDB_Rating").na.drop().show // removes rows containing nulls

     // removing nulls
    moviesDF.select("Title", "IMDB_Rating").na.drop().show // removes rows containing nulls

    // replacing nulls
    moviesDF.na.fill(0, List("IMDB_Rating", "Rotten_Tomatoes_Rating")).show
    moviesDF.na.fill(Map(
        "IMDB_Rating" -> 0,
        "Rotten_Tomatoes_Rating" -> 10,
        "Director" -> "Unknown"
    )).show

    // complex operations
    moviesDF.selectExpr(
        "Title",
        "IMDB_Rating",
        "Rotten_Tomatoes_Rating",
        "ifnull(Rotten_Tomatoes_Rating, IMDB_Rating * 10) as IfNull", // same as coalesce
        "nvl(Rotten_Tomatoes_Rating, IMDB_Rating * 10) as Nvl", // same as coalesce
        "nullif(Rotten_Tomatoes_Rating, IMDB_Rating * 10) as NullIf", // returns NULL if first == second else first
        "nvl2(Rotten_Tomatoes_Rating, IMDB_Rating * 10, 0.0) as Nvl2" // if (first != second) second else third
    ).show

}
