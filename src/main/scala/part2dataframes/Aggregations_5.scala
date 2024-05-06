package part2dataframes

import org.apache.spark.sql.{RelationalGroupedDataset, SparkSession}
import org.apache.spark.sql.functions._

object Aggregations_5 extends App {
  System.setProperty("hadoop.home.dir", "C:\\winutils")

  val spark = SparkSession.builder()
    .appName("Aggregations and Grouping")
    .config("spark.master", "local")
    .getOrCreate()

  val moviesDF = spark.read.json("src/main/resources/data/movies.json")

  // Aggregations

  // 1. Counting... number of rows e.g. count(*)
  val genresCountDF = moviesDF.select(count(col("Major_Genre")))  // all the values except null
  moviesDF.selectExpr("count(Major_Genre)") // is same as above

  // counting all
  moviesDF.select(count("*")) // count all the rows and will INCLUDE NULLs

  // counting distinct
  moviesDF.select(countDistinct(col("Major_Genre")))

  // approximate count -> won't scan entire data
  moviesDF.select(approx_count_distinct(col("Major_Genre")))

  // min and max
  val minRatingDF = moviesDF.select(min(col("IMDB_Rating")))
  moviesDF.selectExpr("min(IMDB_Rating)")

  // sum
  moviesDF.select(sum(col("US_GROSS")))
  moviesDF.selectExpr("sum(US_GROSS)")

  // average
  moviesDF.select(avg(col("US_GROSS")))
  moviesDF.selectExpr("avg(US_GROSS)")

  // data science
  moviesDF.select(
    mean(col("Rotten_Tomatoes_Rating")),  // average
    stddev(col("Rotten_Tomatoes_Rating")) //  how much values are near to mean
  )

  // Grouping

  // count of movies per genre
  val countByGenreDF = moviesDF
    .groupBy(col("Major_Genre"))  // INCLUDES null
    .count()  // select count(*) from moviesDF groupBy Major_Genre

  // avg imdb rating per genre
  val averageRatingByGenre = moviesDF
    .groupBy(col("Major_Genre"))
    .avg("IMDB_Rating")

  val aggregationsByGenreDF = moviesDF
    .groupBy(col("Major_Genre"))
    .agg(
      count("*").as("N_Movies"),
      avg("IMDB_Rating").as("Avg_Rating")
    )
    .orderBy("Avg_Rating")

  /**
    * Exercise:
    * 1. Sum up All the profits of all the movies in the DF
    * 2. Count how many distinct directors we have
    * 3. Show the mean and std deviation of US_GROSS revenue for the movies
    * 4. Compute the avg imdb rating and the average US gross revenue per director
    */

  // Exercise 1: Sum up All the profits of all the movies in the DF
  moviesDF
    .select( col("US_Gross") + col("Worldwide_Gross") + col("US_DVD_Sales") as "Total_Gross")
    .agg(
      sum("Total_Gross") as "All the Profits"
    )
    .show()

  // Exercise 2: Count how many distinct directors we have
  val distinctDirectors = moviesDF
    .select("Director")
    .distinct()
    .count()

  // or
  moviesDF.select(countDistinct("Director")).show()

  println(s"Distinct Directors: ${distinctDirectors}")

  // Exercise 3: Show the mean and std deviation of US_GROSS revenue for the movies
  moviesDF.select("US_Gross")
    .agg(
      mean("US_Gross"),
      stddev("US_Gross")
    ).show()
  // or
  moviesDF.select(mean("US_Gross"), stddev("US_Gross")).show()

  // Exercise 4: Compute the avg imdb rating and the average US gross revenue per director
  moviesDF.groupBy(col("Director"))
    .agg(
      avg(col("IMDB_Rating")) as "Avg_Rating",
      avg(col("US_Gross")) as "Avg_Gross"
    )
    .orderBy(col("Avg_Rating").desc_nulls_last)
    .show()

  /*
    NOTE: Whenever we have to find data per director, use groupBy

    All the Aggregations we have seen in this example were WIDE TRANSFORMATIONS(they require entire data to be scanned).

    Wide Transformations: One or more input partition contribute to one or more output partition
      Data is being shuffled between nodes.
      For faster processing, make sure to do aggregation at the end of entire processing.
   */
}
