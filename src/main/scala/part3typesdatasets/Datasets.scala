package part3typesdatasets

import org.apache.spark.sql.{Dataset, Encoders, SparkSession}
import org.apache.spark.sql.functions._
import java.sql.Date

object Datasets extends App {
    val spark = SparkSession.builder()
      .appName("Datasets")
      .config("spark.master", "local")
      .getOrCreate()

    val numbersDF = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("src/main/resources/data/numbers.csv")

    numbersDF.printSchema()
    // converting DF to DS (a typed DF, with JVM objects)
    implicit val intEncoder = Encoders.scalaInt
    val numberDS: Dataset[Int] = numbersDF.as[Int]

    // dataset of a complex type
    // 1. define your case class
    case class Car(
                  Name: String,
                  Miles_Per_Gallon: Option[Double], // value can be null
                  Cylinders: Long,
                  Displacement: Double,
                  Horsepower: Option[Long],
                  Weight_in_lbs: Long,
                  Acceleration: Double,
                  Year: String,
                  Origin: String
                  )

    // 2. read dataframe
    def readDF(fileName: String) = spark.read.option("inferSchema", "true").json(s"src/main/resources/data/$fileName")

    // 3. define encoder (import implicits)
    import spark.implicits._
    val carsDF = readDF("cars.json")
    // 4. convert DF to DS
    val carsDS = carsDF.as[Car]

    // DS collection functions
    numberDS.filter(_ < 100) // scala like method, instead of dataframe method requiring col or other expression
    // map, flatmap, filter, reduce, fold ...
    val carNamesDS = carsDS.map(car => car.Name.toUpperCase).show

    // Excercise
    // 1. count the number of cars
    val totalCars = carsDS.count()
    println(s"Total cars $totalCars")

    // 2. count powerful cars (horsepower > 140)
    println(carsDS.filter(car => car.Horsepower.getOrElse(0L) > 140).count())

    // 3. average HP of all cars
    val totalHP = carsDS.map(car => car.Horsepower.getOrElse(0L)).reduce((prev, current) => prev+current)
    println(s"Total HP $totalHP")
    val averageHP = totalHP / totalCars
    println(s"Average HP = $averageHP")

    // DS can use DF functions as well
    carsDS.select(avg(col("Horsepower"))).show

    // joins
    // `type` is reserved keyword so renamed `type` field in guitar json to guitarType
    case class Guitar(id: Long, make: String, model: String, guitarType: String)
    case class GuitarPlayer(id: Long, name: String, guitars: Seq[Long], band: Long)
    case class Band(id: Long, name: String, hometown: String, year: Long)

    val guitarDS = readDF("guitars.json").as[Guitar]
    val guitarPlayerDS = readDF("guitarPlayers.json").as[GuitarPlayer]
    val bandDS = readDF("bands.json").as[Band]

    // return type of join is tuple of datasets - Dataset[(_, _)]
    val guitarPlayerBandDS = guitarPlayerDS.joinWith(bandDS, guitarPlayerDS.col("id") === bandDS.col("id"))
    guitarPlayerBandDS.show

    // excercise
    val guitarNGuitarPlayerDS = guitarDS.joinWith(guitarPlayerDS, array_contains(guitarPlayerDS.col("guitars"), guitarDS.col("id")), "outer")
    guitarNGuitarPlayerDS.show

    // grouping
    val carsGroupedByOrigin = carsDS
      .groupByKey(_.Origin)
      .count()
      .show()
}
