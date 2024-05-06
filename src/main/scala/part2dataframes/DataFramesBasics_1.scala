package part2dataframes

import org.apache.spark.sql.types.{DoubleType, LongType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

object DataFramesBasics_1 extends App {

  System.setProperty("hadoop.home.dir", "C:\\winutils")

  // creating a SparkSession
  val spark: SparkSession = SparkSession.builder()
    .appName("DataFramesBasics")
    .config("spark.master", "local")
    .getOrCreate()

  // reading a DF
  val firstDF: DataFrame = spark.read
    .format("json")
    .option("inferSchema", "true")
    .load("src/main/resources/data/cars.json")

  // showing a Data Frame
  firstDF.show()  // will print first 20 rows
  firstDF.printSchema()

  // DF: Distributed collection of rows conforming to a schema
  firstDF.take(10).foreach(println) // get rows

  // spark types
  val longType: LongType.type = LongType

  // schema of an existing dataset
  val carsDFSchema = firstDF.schema
  println(s"Inferred Cars DF Schema: ${carsDFSchema}")

  // schema: Is used mostly. inferSchema is not used much as Spark might take wrong dataType sometimes
  val carsSchema = StructType(Array(
    StructField("Name", StringType),
    StructField("Miles_per_Gallon", DoubleType),
    StructField("Cylinders", LongType),
    StructField("Displacement", DoubleType),
    StructField("Horsepower", LongType),
    StructField("Weight_in_lbs", LongType),
    StructField("Acceleration", DoubleType),
    StructField("Year", StringType),
    StructField("Origin", StringType)
  ))

  // read a DF with your own schema
  val carsDFWithSchema = spark.read
    .format("json")
    .schema(carsSchema)
    .load("src/main/resources/data/cars.json")

  println("Data frame with custom schema...")
  carsDFWithSchema.show()

  // insert row in DF manually.... for testing purpose
  val myRow = Row("chevrolet chevelle malibu",18.0,8L,307.0,130L,3504L,12.0,"1970-01-01","USA")

  // create DF from tuples
  val carTuples = Seq(
    ("chevrolet chevelle malibu",18.0,8L,307.0,130L,3504L,12.0,"1970-01-01","USA"),
    ("buick skylark 320",15.0,8L,350.0,165L,3693L,11.5,"1970-01-01","USA"),
    ("plymouth satellite",18.0,8L,318.0,150L,3436L,11.0,"1970-01-01","USA"),
    ("amc rebel sst",16.0,8L,304.0,150L,3433L,12.0,"1970-01-01","USA"),
    ("ford torino",17.0,8L,302.0,140L,3449L,10.5,"1970-01-01","USA"),
    ("ford galaxie 500",15.0,8L,429.0,198L,4341L,10.0,"1970-01-01","USA"),
    ("chevrolet impala",14.0,8L,454.0,220L,4354L,9.0,"1970-01-01","USA"),
    ("plymouth fury iii",14.0,8L,440.0,215L,4312L,8.5,"1970-01-01","USA"),
    ("pontiac catalina",14.0,8L,455.0,225L,4425L,10.0,"1970-01-01","USA"),
    ("amc ambassador dpl",15.0,8L,390.0,190L,3850L,8.5,"1970-01-01","USA")
  )

  val manualCarsDF: DataFrame = spark.createDataFrame(carTuples) // schema auto-inferred

  // NOTE: DFs have schemas, rows do not

  // create DFs with Implicits
  import spark.implicits._
  // schema auto inferred but with column names now
  val manualCarsDFWithImplicits = carTuples.toDF("Name", "MPG", "Cylinders", "Displacement", "HP", "Weight", "Acceleration", "Year", "Country")

  manualCarsDF.printSchema()
  manualCarsDFWithImplicits.printSchema()

  /*
    Exercise:
    1. Create a manual DF describing smartphones
        - make
        - model
        - screen dimension
        - camera MP

    2. Read another file from the data/ folder e.g. movies.json
      - print its schema
      - count number of rows

   */

  // Exercise 1
  val smartPhoneData = Seq(
    ("iPhone", "14", "720*720", 48),
    ("Samsung", "S20", "1080*1080", 50)
  )

  val smartPhoneDF: DataFrame = smartPhoneData.toDF("make", "model", "screen dimension", "camera MP")
  smartPhoneDF.show()
  smartPhoneDF.printSchema()

  // Exercise 2
  val movieDF: DataFrame = spark.read
    .format("json")
    .option("inferSchema", "true")
    .load("src/main/resources/data/movies.json")

  movieDF.printSchema()
  println(s"movieDF.count(): ${movieDF.count()}")

  // Exercise 2 with StructType
  val movieStructType = StructType(
    List(
      StructField("Creative_Type", StringType),
      StructField("Director", StringType),
      StructField("Distributor", StringType),
      StructField("IMDB_Rating", DoubleType),
      StructField("IMDB_Votes", LongType),
      StructField("MPAA_Rating", StringType),
      StructField("Major_Genre", StringType),
      StructField("Release_Date", StringType),
      StructField("Rotten_Tomatoes_Rating", LongType),
      StructField("Running_Time_min", LongType),
      StructField("Source", StringType),
      StructField("Title", StringType),
      StructField("US_DVD_Sales", LongType),
      StructField("US_Gross", LongType),
      StructField("Worldwide_Gross", LongType)
    )
  )

  val moviesDFFromSchema = spark.read
    .format("json")
    .schema(movieStructType)
    .load("src/main/resources/data/movies.json")

  moviesDFFromSchema.show()
  println(s"movieDFFromSchema: ${moviesDFFromSchema.count()}")
}
