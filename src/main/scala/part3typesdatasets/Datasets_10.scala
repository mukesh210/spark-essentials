package part3typesdatasets

import org.apache.spark.sql.functions.{array_contains, avg, col}
import org.apache.spark.sql.types.{DateType, DoubleType, LongType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Dataset, Encoder, Encoders, RelationalGroupedDataset, SparkSession}

import java.sql.Date

/**
  *   Datasets are Typed DataFrames
  *   Typed DataFrames: Distributed collection of JVM objects
  *
  *   Most Useful when:
  *     We want to maintain type information
  *     We want clean concise code
  *     Our filters/transformations are hard to express in DF or SQL
  *
  *   Avoid when:
  *     performance is critical: Spark can't optimize transformations. So, use DF when performance is critical
  *     but if you want type safety, use Dataset
  */
object Datasets_10 extends App {
  System.setProperty("hadoop.home.dir", "C:\\winutils")

  val spark = SparkSession.builder()
    .appName("Datasets")
    .config("spark.master", "local")
    .getOrCreate()

  // DataFrame is nothing but Dataset[Row]
  val numbersDF: DataFrame = spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv("src/main/resources/data/numbers.csv")

  numbersDF.printSchema()
  /*
    How can we convert numbersDF to Typed DataFrames so that we can direct use higher order
    functions on it and treat it as if we are working with JVM objects.
   */

  // convert DAtaFrame to a Dataset
  implicit val intEncoder: Encoder[Int] = Encoders.scalaInt
  val numbersDS: Dataset[Int] = numbersDF.as[Int]
  // now numbersDS is distributed collection of Ints
  numbersDS.filter(_ < 100)

  val carsSchema = StructType(Array(
    StructField("Name", StringType),
    StructField("Miles_per_Gallon", DoubleType),
    StructField("Cylinders", LongType),
    StructField("Displacement", DoubleType),
    StructField("Horsepower", LongType),
    StructField("Weight_in_lbs", LongType),
    StructField("Acceleration", DoubleType),
    StructField("Year", DateType),
    StructField("Origin", StringType)
  ))

  // Dataset of a complex type
  // 1 - define your case class
  case class Car(
                Name: String,
                Miles_per_Gallon: Option[Double], // put field as Optional if value can be null
                Cylinders: Long,
                Displacement: Double,
                Horsepower: Option[Long],
                Weight_in_lbs: Long,
                Acceleration: Double,
                Year: Date,
                Origin: String
                )

  // 2 - read the DF from the file
  def readDF(fileName: String) = spark.read
    .option("inferSchema", "true")
    .json(s"src/main/resources/data/${fileName}")

  /*
    Encoders.product takes any type that extends Product class. Case classes by default extend Product.
    So, we don't need to do anything additionaly to support Encoders for Car.
    However, it can be tedious to declare encoders for many case classes and so spark provides implicits in package"
    spark.implicits._ through which we don't need to write encoders explicitly for case classes.
   */
//  implicit val CarEncoder = Encoders.product[Car]
  // 3 - define an encoder(importing implicits most of the time)
  import spark.implicits._  // will automatically import all encoders

  val carsDF: DataFrame = spark.read.schema(carsSchema).json("src/main/resources/data/cars.json")

  // 4 - convert DF to DS
  val carsDS: Dataset[Car] = carsDF.as[Car]

  // DS collection functions
  numbersDS.filter(_ < 100)

  // since now we have typed collection, we have access to map, flatMap, fold, reduce, filter, for-comprehension
  val carNameDS: Dataset[String] = carsDS.map(_.Name.toUpperCase)

//  carNameDS.show()

  /**
    * Exercise:
    *   1. Count how many cars we have
    *   2. Count how many powerful cars we have (horsePower > 140)
    *   3. Avg HP for the entire dataset
    */

  // 1. Count how many cars we have
  println(s"Total number of cars: ${carsDS.count()}")

  // 2. Count how many powerful cars we have (horsePower > 140)
  println(s"Count of Powerful cars: ${carsDS.filter(_.Horsepower.exists(_ > 140)).count()}")

  // 3. Avg HP for the entire dataset3. Avg HP for the entire dataset
  val avgHP: Long = carsDS.map(_.Horsepower.getOrElse(0L)).reduce(_ + _) / carsDS.count()
  println(s"Avg HP: ${avgHP}")

  // we can also use DF functions on DataSet!
  carsDS.select(avg(col("Horsepower"))).show

  // Joins
  case class Guitar(id: Long, model: String, make: String, guitarType: String)
  case class GuitarPlayer(id: Long, name: String, guitars: Seq[Long], band: Long)
  case class Band(id: Long, name: String, hometown: String, year: Long)

  val guitarsDS: Dataset[Guitar] = readDF("guitars.json").as[Guitar]
  val guitarPlayerDS: Dataset[GuitarPlayer] = readDF("guitarPlayers.json").as[GuitarPlayer]
  val bandsDS: Dataset[Band] = readDF("bands.json").as[Band]

  // default: inner
  val guitarPlayerBandsDS: Dataset[(GuitarPlayer, Band)] =
    guitarPlayerDS.joinWith(bandsDS, guitarPlayerDS.col("band") === bandsDS.col("id"), "inner")

  guitarPlayerBandsDS
//    .show()

  /**
    *   Exercise:
    *   1. join guitarsDS and guitarPlayersDS on id and guitars(use array_contains), use outer join type
    */
  // Exercise 1:
  val guitarPlayerGuitarDS: Dataset[(GuitarPlayer, Guitar)] =
    guitarPlayerDS.joinWith(guitarsDS, array_contains(guitarPlayerDS.col("guitars"), guitarsDS.col("id")), "outer")

  guitarPlayerGuitarDS
//    .show()

  // Grouping DS
  val carsGroupedByOrigin = carsDS.groupByKey(_.Origin).count()

  carsGroupedByOrigin
//    .show()

  // joins and groups are WIDE transformations, will involve SHUFFLE operations
}
