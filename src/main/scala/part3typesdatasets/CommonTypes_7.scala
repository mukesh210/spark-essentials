package part3typesdatasets

import org.apache.spark.sql.{Column, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.FloatType

object CommonTypes_7 extends App {
  System.setProperty("hadoop.home.dir", "C:\\winutils")

  val spark = SparkSession.builder()
    .appName("Common Spark Types")
    .config("spark.master", "local")
    .getOrCreate()

  val moviesDF = spark.read.json("src/main/resources/data/movies.json")

  // adding a plain value to a DF
  moviesDF.select(col("Title"), lit(47).as("plain_value"))
//    .show()

  // Booleans
  val dramaFilter = col("Major_Genre") === "Drama"
  val goodRatingFilter = col("IMDB_Rating") > 7.0
  val preferredFilters = dramaFilter and goodRatingFilter

  moviesDF.select("Title").where(dramaFilter)
  // + multiple ways of filtering

  val moviesWithGoodnessFlagsDF = moviesDF.select(col("Title"), preferredFilters.as("good_movie"))

  // filter on additional column names
  moviesWithGoodnessFlagsDF.filter("good_movie")  // where col(good_movie) === true

  // negations
  moviesWithGoodnessFlagsDF.where(not(col("good_movie")))

  // Numbers

  // math operators allowed
  val moviesAvgRatingDF = moviesDF
    .select(col("Title"), (col("Rotten_Tomatoes_Rating")/10 + col("IMDB_Rating")) / 2 )

//  moviesAvgRatingDF.show()

  // correlation = number between -1 and 1
  println(s"Correlation: ${moviesDF.stat.corr("Rotten_Tomatoes_Rating", "IMDB_Rating")}") // corr is an action

  // Strings
  val carsDF = spark.read.json("src/main/resources/data/cars.json")

  // capitalization: initcap, lower, upper
  carsDF.select(initcap(col("Name")))
//    .show()

  // contains
  carsDF.select("*").where(col("Name").contains("volkswagen"))

  // regex
  val regexString = "volkswagen|vw"
  val vwDF = carsDF.select(
    col("Name"),
    regexp_extract(col("Name"), regexString, 0).as("regex_extract")
  ).filter(col("regex_extract") =!= "")
//    .show()

  // replace
  vwDF.select(
    col("Name"),
    regexp_replace(col("Name"), regexString, "People's Car").as("regex_replace")
  )
//    .show()

  /**
    * Exercise:
    * 1. Filter the carsDF by a list of car names obtained by an API call
    *   Versions:
    *     - contains
    *     - regex
    */

  def getCarNames: List[String] = List("ford maverick", "peugeot 504", "datsun pl510")

  val carNames = getCarNames

  // version 1 - using regex
  val carNamesRegex = carNames.map(_.toLowerCase).mkString("|")

  carsDF.select(
    col("Name"),
    regexp_extract(col("Name"), carNamesRegex, 0).as("regex_extract")
  )
    .filter(col("regex_extract") =!= "")
//    .show()

  // version 2 - using contains
  val carNameFilters: List[Column] = carNames.map(_.toLowerCase).map(col("Name").contains(_))

  val allCarNames = col("Name") +: carNameFilters
  carsDF.select(
    col("Name"),
    carNameFilters.reduceLeft((acc, elem) => acc or elem).as("contains_filter")
  )
    .where(col("contains_filter"))
    .show()
}
