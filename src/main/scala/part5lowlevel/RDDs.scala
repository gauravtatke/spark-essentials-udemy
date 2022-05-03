package part5lowlevel

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions._

import scala.io.Source

object RDDs extends App {
    val spark = SparkSession.builder()
      .appName("Low level RDDs")
      .config("spark.master", "local")
      .getOrCreate()

    val sc = spark.sparkContext // spark context is needed to operate on RDDs

    // 1. parallelize existing collection
    val numbers = 0 to 1000
    val numbersRDD = sc.parallelize(numbers)

    // 2. reading from file
    case class Stocks(symbol: String, date: String, price: Double)
    def readStocks(fileName: String) = Source.fromFile(fileName)
      .getLines()
      .drop(1) // removing the header
      .map(line => line.split(","))
      .map(token => Stocks(token(0), token(1), token(2).toDouble))
      .toList

    val stocksRDD = sc.parallelize(readStocks("src/main/resources/data/stocks.csv"))
//    stocksRDD.collect().foreach(println)

    // 2b - reading from file
    val stockRDD2 = sc.textFile("src/main/resources/data/stocks.csv")
      .map(line => line.split(","))
      .filter(tokens => tokens(0).toUpperCase() == tokens(0)) // filtering header
      .map(tokens => Stocks(tokens(0), tokens(1), tokens(2).toDouble))

    // 3 - read from a DF
    val stocksDF = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("src/main/resources/data/stocks.csv")

    import spark.implicits._
    val stocksDS = stocksDF.as[Stocks]
    val stocksRDD3 = stocksDS.rdd

    // RDD to DF
    val numbersDF = numbersRDD.toDF("numbers") // loose the type information

    // RDD to DS
    val numbersDS = spark.createDataset(numbersRDD) // keeps the type information

    // transformation
    // distinct and count
//    val msftRDD = stocksRDD.filter(_.

    // transformations
    // count and distinct
    val msftRDD = stocksRDD.filter(_.symbol == "MSFT") // lazy transformation
    val msCount = msftRDD.count()

    val companyNameRDD = stocksRDD.map(_.symbol).distinct() // also lazy

    // min and max
    implicit val stockOrdering: Ordering[Stocks] = Ordering.fromLessThan((sa, sb) => sa.price < sb.price)
    val msftMin = msftRDD.min() // action

    // reduce
    numbersRDD.reduce(_ + _)

    // grouping
    val groupedSStocks = stocksRDD.groupBy(_.symbol) // expensive operation
    groupedSStocks.collect().foreach(println)

    // partitioning
    val repartitionedStockRDD = stocksRDD.repartition(30)
    repartitionedStockRDD.toDF().write.mode(SaveMode.Overwrite).parquet("src/main/resources/data/stocks30")
    /**
      * repartition is expensive, involves shuffling
      * ideal partition size 10-100MB
      */
    // coalesce
    val coalesceStocksRDD = repartitionedStockRDD.coalesce(15) // does NOT involve shuffling
    coalesceStocksRDD.toDF()
      .write
      .mode(SaveMode.Overwrite)
      .parquet("src/main/resources/data/stocks15")

    // Excercises
    // 1. read movies json as RDD with following clase class
    // case class Movie(title: String, genre: String, rating: Double)
    // my solution
    val moviesDF = spark.read
      .option("inferSchema", "true")
      .json("src/main/resources/data/movies.json")

//    case class Movie(Title: String, Major_Genre: String, IMDB_Rating: Option[Double])
//    val moviesDS = moviesDF.as[Movie]
//    val moviesRDD = moviesDS.rdd
//    moviesRDD.take(5).foreach(println)

    // actual solution
    case class Movie(title: String, genre: String, rating: Double)
    val moviesRDD = moviesDF.select(
        col("Title").as("title"),
        col("Major_Genre").as("genre"),
        col("IMDB_Rating").as("rating")
    )
      .where(col("title").isNotNull and col("rating").isNotNull)
      .as[Movie]
      .rdd

    // distinct genres
//    moviesRDD.map(_.Major_Genre).distinct().foreach(println)
    moviesRDD.map(_.genre).distinct().toDF().show()

    // Drama movies with imdb rating > 6
    moviesRDD.filter(movie => movie.genre == "Drama" && movie.rating > 6).toDF().show()

    // avg rating by genre
    case class GenreAvgRating(genre: String, avg_rating: Double)
    moviesRDD.groupBy(_.genre).map {
        case(genre, movies) => GenreAvgRating(genre, movies.map(_.rating).sum / movies.size)
    }
      .toDF()
      .show()

}
