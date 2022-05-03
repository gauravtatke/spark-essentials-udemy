package part2dataframes

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, column, expr}

object ColumnsAndExpressions extends App {

    val spark = SparkSession.builder()
      .appName("DF Columns and Expressions")
      .config("spark.master", "local")
      .getOrCreate()

    val carsDF = spark.read.option("inferSchema", "true").json("src/main/resources/data/cars.json")
    val firstCol = carsDF.col("Name") // returns column object, used in select statement
    val carNamesDF = carsDF.select(firstCol)
    carNamesDF.show()

    // various ways of selecting columns using column object
    import spark.implicits._
    carsDF.select(
        carsDF.col("Name"), // explained above
        col("Acceleration"), // sql function
        column("Origin"),// sql function
        expr("Displacement"),// expression from sql function
        'Year, // scala symbol, auto-converted to column
        $"Horsepower", // fancier interpolated string, returns column using implicit
    ).show(5)

    // using plain strings
    carsDF.select("Name", "Displacement").show(5)

    val simpleExpr = carsDF.col("Weight_in_lbs")
    val weightInKgsExpr = carsDF.col("Weight_in_lbs") / 2.2 // return column object with weights in kg

    carsDF.select(
        col("Name"),
        col("Weight_in_lbs"),
        weightInKgsExpr.as("Weight_in_kgs"),
        expr("Weight_in_lbs / 2.2").as("Weight_in_kg2") // another form of expression
    ).show(5)

    // selectExpr
    carsDF.selectExpr(
        "Name",
        "Weight_in_lbs",
        "Weight_in_lbs / 2.2"
    ).show(5)

    // adding a column
    val carsWithKGColum = carsDF.withColumn("Weight_in_kg3", col("Weight_in_lbs") / 2.2)
    // renaming column
    val carsWithRenamedColumn = carsWithKGColum.withColumnRenamed("Weight_in_kg3", "Weight in kg 3")
    // careful when column names have spaces in them
    carsWithRenamedColumn.selectExpr("`Weight in kg 3`")
    // remove columns
    carsWithRenamedColumn.drop("Displacement", "`Weight in kg 3`")

    // filtering
    carsDF.filter(col("Origin") =!= "USA")
    carsDF.where(col("Origin") =!= "USA")
    // filtering with expression
    carsDF.filter("Origin = 'USA'")
    // chain filters
    carsDF.filter(col("Origin") === "USA").filter(col("Horsepower") > 150)
    carsDF.filter(col("Origin") === "USA" and col("Horsepower") > 150)
    carsDF.filter("Origin = 'USA' and Horsepower > 150")

    // unioning
    val allCarsDF = carsDF.union(
        spark.read.option("inferSchema", "true").json("src/main/resources/data/more_cars.json")
    ) // works if the schema is same

    // distinct
    allCarsDF.select("Origin").distinct().show()
    // Exercises
    val moviesDF = spark.read.option("inferSchema", "true").json("src/main/resources/data/movies.json")
    val moviesDFWithTotalSales = moviesDF
      .na.fill(0, Array("US_DVD_Sales"))
      .withColumn("Total_Sales", col("US_Gross") + col("Worldwide_Gross") + col("US_DVD_Sales"))
    moviesDFWithTotalSales.show()
    val allGoodComedy = moviesDF.filter("Major_Genre = 'Comedy' and IMDB_Rating >= 6")
    allGoodComedy.show()
    moviesDF.select("Title", "IMDB_Rating", "Major_Genre")
      .where("Major_Genre = 'Comedy' and IMDB_Rating > 6")
      .show()
}
