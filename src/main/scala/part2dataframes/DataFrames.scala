package part2dataframes

import org.apache.spark.sql.{Row, SparkSession}

object DataFrames extends App {
    val spark = SparkSession.builder()
      .appName("DataFrames Basics")
      .config("spark.master", "local")
      .getOrCreate()

    val firstDF = spark.read
      .format("json")
      .option("inferSchema", "true")
      .load("src/main/resources/data/cars.json")

   // firstDF.show()
    // firstDF.printSchema()
    firstDF.take(5).foreach(println)
    println(firstDF.schema)

    val cars = Seq(
        ("chevrolet chevelle malibu",18.0,8L,307.0,130L,3504L,12.0,"1970-01-01","USA"),
        ("buick skylark 320",15.0,8L,350.0,165L,3693L,11.5,"1970-01-01","USA"),
        ("plymouth satellite",18.0,8L,318.0,150L,3436L,11.0,"1970-01-01","USA"),
    )
    import spark.implicits._
    val manualCarsDFWithImplicits = cars.toDF(
        "Name", "MilesPerGallon", "Cylinders", "Displacement", "HorsePower", "weight", "Acceleration", "Year", "OriginCountry")

    manualCarsDFWithImplicits.printSchema()
    manualCarsDFWithImplicits.show()

    val manualPhoneDF = Seq(
        ("Samsung", "Galaxy A10", "6.1", 12),
        ("Samsung", "Galaxy S9", "6.5", 14),
        ("Google", "Pixel", "5.5", 12),
    ).toDF("Make", "Model", "ScreenSize", "CameraMP")

    manualPhoneDF.show()

    val moviesDF = spark.read
      .format("json")
      .option("inferSchema", "true")
      .load("src/main/resources/data/movies.json")

    moviesDF.printSchema()
    moviesDF.count() // need to use println, otherwise it does not show
    println(s"Num of rows: ${moviesDF.count()}")
}
