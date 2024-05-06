package part2dataframes

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

/*
  Joins are WIDE TRANSFORMATION
 */
object Joins_6 extends App {
  System.setProperty("hadoop.home.dir", "C:\\winutils")

  val spark = SparkSession.builder()
    .appName("Joins")
    .config("spark.master", "local")
    .getOrCreate()

  val guitarsDF = spark.read.json("src/main/resources/data/guitars.json")
  val guitaristDF = spark.read.json("src/main/resources/data/guitarPlayers.json")
  val bandsDF = spark.read.json("src/main/resources/data/bands.json")

  // joins

  // inner join
  val joinCondition = guitaristDF.col("band") === bandsDF.col("id")
  val guitaristsBandsDF = guitaristDF.join(bandsDF, joinCondition, "inner")

//  guitaristsBandsDF.show()

  // outer join
  // left outer join = everything in inner join + all the rows in the LEFT table,
  // with nulls in where data is missing
  guitaristDF.join(bandsDF, joinCondition, "left_outer")
//    .show()

  // right outer join = everything in inner join + all the rows in the RIGHT table,
  // with nulls in where data is missing
  guitaristDF.join(bandsDF, joinCondition, "right_outer")
//    .show()

  // full outer join  = everything in inner join + all the rows in both tables,
//  with nulls in where the data is missing
  guitaristDF.join(bandsDF, joinCondition, "outer")
//    .show()

  // semi-joins = everything in the left DF for which there is a row in the right DF satisfying the condition
  guitaristDF.join(bandsDF, joinCondition, "left_semi")
//    .show()

  // anti-joins = everything in the left DF for which there is NO row in the right DF satisfying the condition
  guitaristDF.join(bandsDF, joinCondition, "left_anti")
//    .show()

  // things to bear in mind while doing JOIN

  // will CRASH since left and right table both have same name columns,
  // so spark is not sure which column we are referring to
//  guitaristsBandsDF.select("id", "band")
//    .show()

  // solution:
  // option 1: Rename the columns on which we are joining
  guitaristDF.join(bandsDF.withColumnRenamed("id", "band"), "band")
//    .show()

  // option 2: drop the duplicate column
  guitaristsBandsDF.drop(bandsDF.col("id"))
//    .show()

  // option 3: rename the offending column and keep the data
  val bandsModDF = bandsDF.withColumnRenamed("id", "bandId")
  guitaristDF.join(bandsModDF, guitaristDF.col("band") === bandsModDF.col("bandId"))
//    .show()

  // using complex types
  guitaristDF.join(guitarsDF.withColumnRenamed("id", "guitarId"), expr("array_contains(guitars, guitarId)"))
//    .show()

  /**
    * Exercise:
    * 1. Show all employees and their max salary
    * 2. Show all employees who were never managers
    * 3. find the job title of the best paid 10 employees in the company
    *
    */

  val driver = "org.postgresql.Driver"
  val url = "jdbc:postgresql://localhost:5432/rtjvm"
  val user = "docker"
  val password = "docker"
  def dbConnectionDetails(dbtable: String) = Map(
    "driver" -> driver,
    "url" -> url,
    "user" -> user,
    "password" -> password,
    "dbtable" -> dbtable)
  val employeesDF = spark.read.format("jdbc")
    .options(dbConnectionDetails("public.employees")).load()

  val salariesDF = spark.read.format("jdbc")
    .options(dbConnectionDetails("public.salaries")).load()

  val deptManagerDF = spark.read.format("jdbc")
    .options(dbConnectionDetails("public.dept_manager")).load()

  val titlesDF = spark.read.format("jdbc")
    .options(dbConnectionDetails("public.titles")).load()

  // Exercise 1: Show all employees and their max salary
  val maxSalaryPerEmployeeDF = employeesDF.join(salariesDF, "emp_no")
    .groupBy("emp_no")
    .agg(max("salary") as "Max_Salary")
    .orderBy(col("Max_Salary").desc)

  maxSalaryPerEmployeeDF
//    .show()
  // OR
  val maxSalariePerEmployeeDF2 = salariesDF.groupBy("emp_no").agg(max("salary") as "Max_Salary")

  val employeeSalaryDF = employeesDF.join(maxSalariePerEmployeeDF2, "emp_no")
//    .show()

  // Exercise 2: Show all employees who were never managers
  val employeesDeptJoinCondition = employeesDF.col("emp_no") === deptManagerDF.col("emp_no")
  employeesDF.join(deptManagerDF, employeesDeptJoinCondition, "left_anti")
//    .show()

  // Exercise 3: find the job title of the best paid 10 employees in the company
  val mostRecentJobTitleDF = titlesDF.groupBy("emp_no", "title").agg(max("to_date"))
  val bestPaidEmployeesDF = employeeSalaryDF.orderBy(col("Max_Salary").desc).limit(10)
  val bestPaidJobsDF = bestPaidEmployeesDF.join(mostRecentJobTitleDF, "emp_no")

  bestPaidJobsDF.show()
}
