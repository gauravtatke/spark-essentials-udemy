package part2dataframes

import org.apache.spark.sql.types.{DateType, DoubleType, LongType, StringType, StructField, StructType}
import org.apache.spark.sql.{SaveMode, SparkSession}

object DataSources extends App {
    val spark = SparkSession.builder()
      .appName("Data Sources")
      .config("spark.master", "local")
      .getOrCreate()

    val carsDF = spark.read
      .format("json")
      .options(Map(
          "mode" -> "failFast",
          "path" -> "src/main/resources/data/cars.json",
          "inferSchema" -> "true"
      ))
      .load()

//    carsDF.show()

//    carsDF.write
//      .format("json")
//      .mode(SaveMode.Overwrite)
//      .save("src/main/resources/data/cars_dupe.json") // it creates a folder with this name


    val carsSchema = StructType(Array(
        StructField("Name", StringType),
        StructField("Miles_per_Gallon", DoubleType),
        StructField("Cylinders", LongType),
        StructField("Displacement", DoubleType),
        StructField("Horsepower", LongType),
        StructField("Weight_in_lbs", LongType),
        StructField("Acceleration", DoubleType),
        StructField("Year", DateType), // changed from string to date type
        StructField("Origin", StringType)
    ))

    // json options - popular ones
    spark.read
      .schema(carsSchema)
      .option("dateFormat", "yyyy-MM-dd")
      .option("allowSingleQuotes", "true")
      .option("compression", "uncompressed")
      .json("src/main/resources/data/cars.json")
//      .show(5)

    // csv options - common ones
    val stockSchema = StructType(Array(
        StructField("symbol", StringType),
        StructField("date", DateType),
        StructField("price", DoubleType)
    ))
    spark.read
      .schema(stockSchema)
      .option("sep", ",")
      .option("header", "true")
      .option("nullValue", "")
      .option("dateFormat", "MMM d yyyy")
      .csv("src/main/resources/data/stocks.csv")
//      .show(5)

    carsDF.write
      .mode(SaveMode.Overwrite)
      .save("src/main/resources/data/cars.parquet")

    // reading from remote DB
    val employeesDF = spark.read
      .format("jdbc")
      .option("driver", "org.postgresql.Driver")
      .option("url", "jdbc:postgresql://localhost:5432/rtjvm") // this is set in docker
      .option("user", "docker")
      .option("password", "docker")
      .option("dbtable", "public.employees")
      .load()

    employeesDF.show(5)

    // exercise 1
    val moviesDF = spark.read.json("src/main/resources/data/movies.json")
    moviesDF.show(3)

    moviesDF.write
      .mode(SaveMode.Overwrite)
      .save("src/main/resources/data/movies.parquet")

    moviesDF.write
      .format("csv")
      .option("sep", "\t")
//      .option("sep", "  ")
      .option("header", "true")
      .mode(SaveMode.Overwrite)
      .save("src/main/resources/data/movies.csv")

    moviesDF.write
      .format("jdbc")
      .option("driver", "org.postgresql.Driver")
      .option("url", "jdbc:postgresql://localhost:5432/rtjvm") // this is set in docker
      .option("user", "docker")
      .option("password", "docker")
      .option("dbtable", "public.movies_gtatke")
      .save()

}
