package part3typesdatasets

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object ComplexTypes_8 extends App {
  System.setProperty("hadoop.home.dir", "C:\\winutils")

  val spark = SparkSession.builder()
    .appName("Complex data types")
    .config("spark.master", "local")
    .getOrCreate()

  val moviesDF = spark.read.option("inferSchema", "true").json("src/main/resources/data/movies.json")

  // Dates

  // what if someone is already reading DF with inferSchema: true and it is reading dates as String,
  // in this case, how to treat date column as date dataType.
  // Idea is to select the column as date using to_date function and provide some formatter
  spark.sql("set spark.sql.legacy.timeParserPolicy=LEGACY")

  val moviesWithReleaseDates = moviesDF
    .select(col("Title"), to_date(col("Release_Date"), "dd-MMM-yy").as("Actual_Release")) // to_date: conversion

  moviesWithReleaseDates
    .withColumn("Today", current_date()) // today
    .withColumn("Right_Now", current_timestamp()) // this second
    .withColumn("Movie_Age", datediff(col("Today"), col("Actual_Release")) / 365) // date differences
  // Other utility functions: date_add, date_sub

  // some movies might have different format of date, so spark puts null in them if it is unable to parse them
  moviesWithReleaseDates.select("*").where(col("Actual_Release").isNull)
  //    .show()

  /**
    * Exercise:
    *   1. How do we deal with multiple formats
    *      2. Read the stocks DF and parse the dates correctly
    *
    */

  // Exercise 1
  /*
    1. Parse the DF multiple times, then union the small DFs
   */

  // Exercise 2
  val stocksDF = spark.read
    .option("inferSchema", "true")
    .option("header", "true")
    .option("sep", ",")
    .option("nullValue", "")
    .csv("src/main/resources/data/stocks.csv")

  val stocksDFWithDates = stocksDF
    .withColumn("Actual_Date", to_date(col("date"), "MMM dd yyyy"))

  //  stocksDFWithDates.show()

  // Structures: Groups of column aggregated into one
  // In the movies.json, we will show US_GROSS and WorldWide_GROSS in one column

  // 1. with col operators
  moviesDF
    .select(col("Title"), struct(col("US_Gross"), col("Worldwide_Gross")).as("Profit"))
    .select(col("Title"), col("Profit").getField("US_Gross").as("US_Profit"))
//    .show()
  // so Structures are like tuples composed of multiple values

  // 2 - with expression strings
  moviesDF
    .selectExpr("Title", "(US_Gross, Worldwide_Gross) as Profit")
    .selectExpr("Title", "Profit.US_Gross as US_Profit")
//    .show()

  // Arrays
  val moviesWithWordsDF = moviesDF
    .select(col("Title"), split(col("Title"), " |,").as("Title_Words")) // ARRAY of strings

  moviesWithWordsDF.select(
    col("Title"),
    expr("Title_Words[0]"),
    size(col("Title_Words")),
    array_contains(col("Title_Words"), "Love")
  )
    .show()

}
