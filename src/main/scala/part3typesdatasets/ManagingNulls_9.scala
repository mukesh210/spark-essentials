package part3typesdatasets

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

/*
  Whenever we use inferSchema: true, Spark automatically assumes that column might be nullable,
  so in the generated schema, we see nullable=true

  Whereas if we enforce schema on a DF and use nullable=false, we must be pretty sure that value
  can't be null. For non-nullable columns, spark internally does some optimization.
  It can lead to exceptions or data error if broken.
 */
object ManagingNulls_9 extends App {
  System.setProperty("hadoop.home.dir", "C:\\winutils")

  val spark = SparkSession.builder().appName("Managing Nulls").config("spark.master", "local").getOrCreate()

  val moviesDF = spark.read.option("inferSchema", "true").json("src/main/resources/data/movies.json")

  // select the first non-null value(out of rotten tomatoes rating & imdb rating)
  moviesDF.select(
    col("Title"),
    col("Rotten_Tomatoes_Rating"),
    col("IMDB_Rating"),
    coalesce(col("Rotten_Tomatoes_Rating"), col("IMDB_Rating") * 10)
  )

  // checking for nulls
  moviesDF.select("*").where(col("Rotten_Tomatoes_Rating").isNull)

  // nulls when ordering
  moviesDF.select(col("Title"), col("IMDB_Rating")).orderBy(col("IMDB_Rating").desc_nulls_last)

  // removing rows where values are null
  moviesDF.select("Title", "IMDB_Rating").na.drop() // remove rows containing nulls

  // replace nulls
  moviesDF.na.fill(0, List("IMDB_Rating", "Rotten_Tomatoes_Rating"))
  moviesDF.na.fill(Map(
    "IMDB_Rating" -> 0,
    "Rotten_Tomatoes_Rating" -> 10,
    "Director" -> "Unknown"
  ))

  // complex operations: below functions aren't available in DF API, only available in sql api
  moviesDF.selectExpr(
    "Title",
    "Rotten_Tomatoes_Rating",
    "IMDB_Rating",
    "ifnull(Rotten_Tomatoes_Rating, IMDB_Rating * 10) as ifnull",  // same as coalesce
    "nvl(Rotten_Tomatoes_Rating, IMDB_Rating * 10) as nvl", // same as coalesce
    "nullif(Rotten_Tomatoes_Rating, IMDB_Rating * 10) as nullif",  // returns null if the two values are EQUAL, else return first value
    "nvl2(Rotten_Tomatoes_Rating, IMDB_Rating * 10, 0.0) as nvl2" // if(first != null) second else third
  )
    .show()
}
