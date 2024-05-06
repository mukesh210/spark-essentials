package part2dataframes

import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

object DataSources_3 extends App {
  System.setProperty("hadoop.home.dir", "C:\\winutils")

  val spark = SparkSession.builder()
    .appName("Data Sources and Formats")
    .config("spark.master", "local")
    .getOrCreate()

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

  /*
    Reading a DF:
      - format
      - schema or inferSchema = true
      - zero or more options
   */
  val carsDF: DataFrame = spark.read
    .format("json")
    .schema(carsSchema) // enforce a schema
    // failFast if we encounter malformed record
    .option("mode", "failFast") // other options: dropMalformed, permissive(default)
    .option("path", "src/main/resources/data/cars.json")
    .load()

  // alternative to above(with option Map)
  val carsDFwithOptionMap = spark.read
    .format("json")
    .options(Map(
      "mode" -> "failFast",
      "path" -> "src/main/resources/data/cars.json",
      "inferSchema" -> "true"
    ))
    .load()

  /*
    Writing DFs
      - format
      - save mode = override, append, ignore, errorIfExists
      - path
      - zero or more options
   */

  carsDF.write
    .format("json")
    .mode(SaveMode.Overwrite)
    .option("path", "src/main/resources/data/cars_dupe.json")
    .save()
  // Since we are dealing with Big Data, save might result in multiple partitioned data saved.

  // JSON flags
  val carsSchemaWithDateType = StructType(Array(
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

  val carsDFWithDateType: DataFrame = spark.read
    .schema(carsSchemaWithDateType)
    .option("dateFormat", "yyyy-MM-dd") // couple with schema; if spark fails parsing, it will put null
    .option("allowSingleQuotes", "true")
    .option("compression", "uncompressed") // other values: bzip2, gzip, lz4, snappy, deflate
    .json("src/main/resources/data/cars.json") // json method will load the file as well(no need to use format method)

  carsDFWithDateType.show()
  carsDFWithDateType.printSchema()

  // CSV flags
  val stocksSchema = StructType(Array(
    StructField("symbol", StringType),
    StructField("date", DateType),
    StructField("price", DoubleType)
  ))

  val stocksDF = spark.read
    .schema(stocksSchema)
    .option("dateFormat", "MMM d yyyy")
    .option("header", "true")
    .option("sep", ",")
    .option("nullValue", "")
    .csv("src/main/resources/data/stocks.csv") // no need to use format method, csv will load the data as well

  stocksDF.printSchema()
  stocksDF.show()

  // Parquet: open source compressed binary storage format optimized for fast reading of columns
  // Parquet is default mode
  carsDF.write
    .mode(SaveMode.Overwrite)
    //    .parquet("src/main/resources/data/cars.parquet")  // is equivalent to
    .save("src/main/resources/data/cars.parquet") // since by default spark saves data in parquet format

  // txt files -> every line is separate row
  spark.read.text("src/main/resources/data/sampleTextFile.txt").show()

  // Reading from a remote DB
  val driver = "org.postgresql.Driver"
  val url = "jdbc:postgresql://localhost:5432/rtjvm"
  val user = "docker"
  val password = "docker"
  val employeesDF = spark.read
    .format("jdbc")
    .option("driver", driver)
    .option("url", url)
    .option("user", user)
    .option("password", password)
    .option("dbtable", "public.employees")
    .load()

  // will pull data from db
  //  employeesDF.printSchema()
  //  employeesDF.show()

  /**
    * Exercise:
    *   1. Read movies.json as DF, then write it as
    *     - tab separated values file(csv)
    *     - snappy parquet
    *     - table "public.movies" in the postgres DB
    *
    */

  val moviesDF = spark.read.json("src/main/resources/data/movies.json")

  moviesDF.show()

  // save moviesDF in CSV format
  moviesDF.write
    .mode(SaveMode.Overwrite)
    .option("sep", "\t")
    .option("header", "true")
    .csv("src/main/resources/data/moviesAsCSV.csv")

  // save moviesDF in parquet format
  moviesDF.write
    .mode(SaveMode.Overwrite)
    .option("compression", "snappy") // default, so can be ignored
    .parquet("src/main/resources/data/moviesAsParquert.parquet")

  // save moviesDF in "public.movies" table in postgresql
  moviesDF.write
    .format("jdbc")
    .option("driver", driver)
    .option("url", url)
    .option("user", user)
    .option("password", password)
    .option("dbtable", "public.movies")
    .save()
}
