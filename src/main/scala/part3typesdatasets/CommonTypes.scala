package part3typesdatasets

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object CommonTypes extends App {
    val spark = SparkSession.builder()
      .appName("Common DataTypes")
      .config("spark.master", "local")
      .getOrCreate()

    val moviesDF = spark.read.option("inferSchema", "true").json("src/main/resources/data/movies.json")

    // adding a plain value to column
    moviesDF.select(col("Title"), lit(47).as("plain_value")).show()

    // boolean
    val dramaFilter = col("Major_Genre") equalTo "Drama" // col("Major_Genre") === "Drama"
    moviesDF.select(col("Title")).where(dramaFilter).show() // drama movies

    val goodRatingFilter = col("IMDB_Rating") > 7.0
    val preferredFilter = dramaFilter and goodRatingFilter
    val moviesWithGoodnessFlagDF = moviesDF.select(col("Title"), preferredFilter.as("good_movies"))
    moviesWithGoodnessFlagDF.where("good_movies").show()

    // negation
    moviesWithGoodnessFlagDF.where(not(col("good_movies")))

    // numbers, math operators
    val moviesAvgRatingsDF = moviesDF.select(col("Title"), (col("Rotten_Tomatoes_Rating") / 10 + col("IMDB_Rating")) / 2)
    moviesAvgRatingsDF.show

    // correlation - number between -1 and 1
    println(moviesDF.stat.corr("Rotten_Tomatoes_Rating", "IMDB_Rating")) // correlation is an action

    // String
    val carsDF = spark.read.option("inferSchema", "true").json("src/main/resources/data/cars.json")
    carsDF.select(initcap(col("Name"))).show // capitalization, lower, upper etc.
    carsDF.select("*").where(col("Name").contains("volkswagen")).show

    // regex
    val regexString = "volkswagen|vw"
    val vwDF = carsDF.select(
        col("Name"),
        regexp_extract(col("Name"), regexString, 0).as("regex_extract")
    ).where(col("regex_extract") =!= "")

    vwDF.select(
        col("Name"),
        regexp_replace(col("Name"), regexString, "People's Car").as("regex_replace")
    ).show

    // excercise
    def getCarNames: List[String] = List("volkswagen", "ford")
    var listRegex = ""
    for(carName <- getCarNames) {
        listRegex = listRegex + "|" + carName
    }

    listRegex = if (listRegex.startsWith("|")) {
        listRegex.substring(1)
    } else listRegex
    println(listRegex)

    carsDF.select(
        col("Name"),
        regexp_extract(col("Name"), listRegex, 0).as("filtered_column")
    ).where(col("filtered_column") =!= "")
      .drop("filtered_column")
      .show()

    // version 2 - using contains
    val carNameFilter = getCarNames.map(_.toLowerCase()).map(name => col("Name").contains(name))
    val bigFilter = carNameFilter.fold(lit(false))((combinedFilter, newCarNameFilter) => combinedFilter or newCarNameFilter)
    carsDF.filter(bigFilter).show
}