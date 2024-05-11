package part5lowlevel

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, Dataset, Row, SaveMode, SparkSession}

import scala.io.Source

/*
  RDDs: Resilient distributed datasets
  Distributed typed collection of JVM objects.

  Same definition as DataSets, then what is the diff b/t RDDs and DataSets?
  RDDs are "first citizen" of Spark: all higher-level APIs are reduced to RDDs

  Pros: RDDs can be highly optimized
    1. Partitioning can be controlled
    2. order of elements can be controlled
    3. order of operations matter for performance

  Cons: Hard to work with
    1. for complex operations, need to know the internals of Spark
    2. Poor APIs for quick data processing

  For 99% of operations, use the DataFrame/Dataset APIs
 */
object RDDs_13 extends App {
  System.setProperty("hadoop.home.dir", "C:\\winutils")

  val spark = SparkSession.builder()
    .appName("Introduction to RDDs")
    .config("spark.master", "local")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")

  val sc = spark.sparkContext

  // Various ways of creating RDDs
  // 1. Parallelize an existing collection
  val numbers = 1 to 1000000
  val numbersRDD: RDD[Int] = sc.parallelize(numbers)

  // 2. reading from files
  case class StockValue(symbol: String, date: String, price: Double)
  def readStocks(fileName: String): List[StockValue] =
    Source.fromFile(fileName)
      .getLines()
      .drop(1)
      .map(line => line.split(","))
      .map(tokens => StockValue(tokens(0), tokens(1), tokens(2).toDouble))
      .toList

  val stocksRDD: RDD[StockValue] = sc.parallelize(readStocks("src/main/resources/data/stocks.csv"))

  // 2b: reading from file
  val stocksRDD2: RDD[StockValue] = sc.textFile("src/main/resources/data/stocks.csv")
    .map(line => line.split(","))
    .filter(tokens => tokens(0).toUpperCase == tokens(0)) // for filtering out header
    .map(tokens => StockValue(tokens(0), tokens(1), tokens(2).toDouble))

  // 3 - read from a DF
  val stocksDF: DataFrame = spark.read
    .options(Map(
      "header" -> "true",
      "inferSchema" -> "true"
    ))
    .csv("src/main/resources/data/stocks.csv")
  val stocksRDD4: RDD[Row] = stocksDF.rdd // don't have type information... for type info use DS

  import spark.implicits._
  val stocksDS = stocksDF.as[StockValue]
  val stocksRDD3: RDD[StockValue] = stocksDS.rdd

  // 4 - convert RDD to DF
  val numbersDF: DataFrame = numbersRDD.toDF("numbers") // you lose the type information

  // RDD -> DS
  val numbersDS: Dataset[Int] = spark.createDataset(numbersRDD) // preserve type information

  // RDD Transformation

  // counting
  val msftRDD: RDD[StockValue] = stocksRDD.filter(_.symbol == "MSFT")  // lazy transformation
  val msftCount = msftRDD.count() // EAGER ACTION

  // distinct
  val companyNamesEDD = stocksRDD.map(_.symbol).distinct() // also lazy transformation

  // min and max
  implicit val stockOrdering: Ordering[StockValue] = Ordering.fromLessThan[StockValue]((sa, sb) => sa.price < sb.price)
  val minMsft = msftRDD.min()   // action

  // reduce
  numbersRDD.reduce(_ + _)

  // grouping
  val groupedStocksRDD = stocksRDD.groupBy(_.symbol)
  // VERY EXPENSIVE bcz it involves SHUFFLING

  // Partitioning
  val repartitionedStocksRDD: RDD[StockValue] = stocksRDD.repartition(5)
  repartitionedStocksRDD.toDF().write
    .mode(SaveMode.Overwrite)
    .parquet("src/main/resources/data/stocks5") // will save 5 files since we have numPartitions as 5
  /*
    Repartitioning is EXPENSIVE. Involves SHUFFLING.
    Best Practice: Do Partition EARLY, then process later.
      Size of a partition: 10-100MB
   */

  // coalesce: Repartition a RDD to fewer partition than it currently have
  val coalescedRDD = repartitionedStocksRDD.coalesce(2)   // does not involve SHUFFLING
  coalescedRDD.toDF().write
    .mode(SaveMode.Overwrite)
    .parquet("src/main/resources/data/stocks2")

  /**
    * Exercises:
    * 1. Read the movies.json as an RDD
    * 2. Show the distinct genre as an RDD
    * 3. Select all the movies in the Drama genre with IMDB rating > 6
    * 4. Show the average rating of movies by genre.
    */

  case class Movie(title: String, genre: String, rating: Double)

  // Exercise 1: Read the movies.json as an RDD
  def removeFirstAndLast(str: String) = str.drop(1).dropRight(1)

  def parseRating(str: String) = if(str == "null") 0d else str.toDouble

  val moviesDF = spark.read.option("inferSchema", "true").json("src/main/resources/data/movies.json")

  val moviesRDD: RDD[Movie] = moviesDF
    .select(col("Title") as "title", col("Major_Genre") as "genre", col("IMDB_Rating") as "rating")
    .where(col("genre").isNotNull and col("rating").isNotNull)
    .as[Movie]
    .rdd
  moviesRDD.toDF().show()

  // Exercise 2: Show the distinct genre as an RDD
  val distinctGenreRDD: RDD[String] = moviesRDD.map(_.genre).distinct()
  distinctGenreRDD.toDF().show()

  // Exercise 3: Select all the movies in the Drama genre with IMDB rating > 6
  val dramaGenreRDD = moviesRDD.filter(movie => movie.genre == "Drama" && movie.rating > 6)
  dramaGenreRDD.toDF().show()

  // Exercise 4: Show the average rating of movies by genre.
  val avgRatingByGenreRDD = moviesRDD.groupBy(_.genre).mapValues { movieList =>
      val ratingSum = movieList.map(_.rating).sum
      ratingSum / movieList.size
  }
    .sortBy(_._2)
  avgRatingByGenreRDD.toDF().show()



  /*
    RDDs vs DataSets
      DataFrame == Dateset[Row]

      Commonality between RDDs and Datasets:
      1. Collection APIs: map, flatMap, filter, take, reduce, etc.
      2. union, count, distinct
      3. groupBy, sortBy

      RDDs over Datasets
      1. Partition Control: Repartition, coalesce, partitioner, zipPartitions, mapPartitions
      2. operation control: checkpoint, isCheckpointed, localCheckpoint, cache
      3. storage control: cache, getStorageLevel, persist

      Datasets over RDDs
      1. Select and Join!
      2. Spark Planning/optimization before running code

      For 99% of operations, use the Dataframe/Dataset APIs
   */
}
