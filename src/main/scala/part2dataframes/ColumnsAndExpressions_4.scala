package part2dataframes

import org.apache.spark.sql._

object ColumnsAndExpressions_4 extends App {
  System.setProperty("hadoop.home.dir", "C:\\winutils")

  val spark = SparkSession.builder()
    .appName("DF Columns and Expressions")
    .config("spark.master", "local")
    .getOrCreate()

  val carsDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/cars.json")

  // Columns
  val carNameColumn: Column = carsDF.col("Name")

  // selecting column (also known as PROJECTION)
  val carNamesDF: DataFrame = carsDF.select(carNameColumn) // select Name from carsDF
  //  carNamesDF.show()

  // various select methods

  import org.apache.spark.sql.functions.{col, column, expr}
  import spark.implicits._

  carsDF.select(
    carsDF.col("Name"),
    col("Acceleration"), // need import
    column("Weight_in_lbs"), // need import
    'Year, // scala Symbol, auto-converted to column(need spark.implicits._ import to work)
    $"HorsePower", // fancier interpolated string, returns a column object
    expr("Origin") // Expression
  )

  // select with Plain column names
  carsDF.select("Name", "Year")

  // Selecting column name is an EXPRESSIONS
  // More EXPRESSIONS

  val simpleExpression: Column = carsDF.col("Weight_in_lbs")
  val weightInKgExpression: Column = carsDF.col("Weight_in_lbs") / 2.2

  val carsWithWeightsDF = carsDF.select(
    col("Name"),
    col("Weight_in_lbs"),
    weightInKgExpression.as("Weight_in_kg"),
    expr("Weight_in_lbs / 2.2").as("Weight_in_kg2")
  )

  // if we have lots of transformation in above select clause... like multiple expr call in select method,
  // it can be represented as "selectExpr"
  val carsWithSelectExprWeightsDF = carsDF.selectExpr(
    "Name",
    "Weight_in_lbs",
    "Weight_in_lbs / 2.2"
  )

  // DF processing

  // adding a column to DF
  val carsWithKg3DF: DataFrame = carsDF.withColumn("Weight_in_kg_3", col("Weight_in_lbs") / 2.2)

  // renaming a column
  val carsWithColumnRenamed = carsDF.withColumnRenamed("Weight_in_lbs", "Weight in pounds")
  // careful with column names... if column names have space or ., use `` operator
  carsWithColumnRenamed.selectExpr("`Weight in pounds`")
  // remove a column
  carsWithColumnRenamed.drop("Cylinders", "Displacement")

  // filtering
  val europianCarsDF: Dataset[Row] = carsDF.filter(col("Origin") =!= "USA")
  val europianCarsDF2: Dataset[Row] = carsDF.where(col("Origin") =!= "USA")

  // filtering with expression strings
  val americanCarsDF = carsDF.filter("Origin = 'USA' ")
  val americanCarsDF2 = carsDF.filter(col("Origin") === "USA")

  // chain filters
  val americanPowerfulCarsDF = carsDF.filter(col("Origin") === "USA").filter(col("HorsePower") > 150)
  val americanPowerfulCarsDF2 = carsDF.filter(col("Origin") === "USA" and col("HorsePower") > 150)
  val americanPowerfulCarsDF3 = carsDF.filter("Origin = 'USA' and HorsePower > 150")

  // unioning = adding more rows
  val moreCarsDF = spark.read.json("src/main/resources/data/more_cars.json")
  val allCarsDF = carsDF.union(moreCarsDF) // works if DFs have same schema

  // distinct
  val allCountriesDF = carsDF.select("Origin").distinct()
  allCountriesDF.show()

  /**
    * Exercises:
    * 1. Read movies.json and select any 2 columns(title, release date)
    * 2. Create another column summing up the total profit of the movies = US_Gross + Worldwide_Gross + DVD_Sales
    * 3. Select all COMEDY movies with IMDB rating above 6
    *
    * Use as many versions as possible
    */

  // Exercise 1
  val moviesDF: DataFrame = spark.read.json("src/main/resources/data/movies.json")

  // different ways to select columns
  val moviesDF2 = moviesDF.select("Title", "Release_Date")
  val moviesDF3 = moviesDF.select(
    col("Title"),
    $"Release_Date"
  )
  val moviesDF4 = moviesDF.select(expr("Title"), expr("Release_Date"))
  val moviesDF5 = moviesDF.select(column("Title"), 'Release_Date)
  val moviesDF6 = moviesDF.selectExpr("Title", "Release_Date")

  // Exercise 2
  // 1st way
  val moviesDFWithProfits = moviesDF
    .select("US_Gross", "Worldwide_Gross", "US_DVD_Sales")
    .withColumn("Total_Profit",
      col("US_Gross") + col("Worldwide_Gross") + col("US_DVD_Sales"))

  moviesDFWithProfits
    .filter("Total_Profit > 0")
    .show()

  // 2nd way
  val moviesDFwithProfits2 = moviesDF.select(
    col("Title"),
    col("US_Gross"),
    col("Worldwide_Gross"),
    (col("US_Gross") + col("Worldwide_Gross")).as("Total_Profit_2")
  )

  moviesDFwithProfits2.filter("Total_Profit_2 > 0").show()

  // 3rd way
  val moviesDFWithProfit3 = moviesDF.selectExpr(
    "Title",
    "US_Gross",
    "Worldwide_Gross",
    "US_DVD_Sales",
    "US_Gross + Worldwide_Gross + US_DVD_Sales as Total_Profit_3"
  )

  moviesDFWithProfit3.filter("Total_Profit_3 > 0").show()

  // Exercise 3
  val mediocreComediesDF = moviesDF.select("Title", "IMDB_Rating")
    .where("Major_Genre = 'Comedy' ")
    .filter("IMDB_Rating > 6")

  val mediocreComediesDF2 = moviesDF.select("Title", "IMDB_Rating")
    .where(col("Major_Genre") === "Comedy" and col("IMDB_Rating") > 6)

  val mediocreComediesDF3 = moviesDF.select("Title", "IMDB_Rating")
    .where(col("Major_Genre") === "Comedy")
    .where(col("IMDB_Rating") > 6)

  val mediocreComediesDF4 = moviesDF.select("Title", "IMDB_Rating")
    .where("Major_Genre = 'Comedy' and IMDB_Rating > 6")

  mediocreComediesDF3.show()
}
