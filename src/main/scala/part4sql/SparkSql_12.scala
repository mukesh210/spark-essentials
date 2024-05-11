package part4sql

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.functions._

object SparkSql_12 extends App {
  System.setProperty("hadoop.home.dir", "C:\\winutils")

  val spark = SparkSession.builder()
    .appName("Spark SQL Practice")
    .config("spark.master", "local")
    // spark will save db at this directory
    .config("spark.sql.warehouse.dir", "src/main/resources/warehouse")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")

  val carsDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/cars.json")

  // regular DF API for selecting and filtering data
  carsDF.select(col("Name")).where(col("Origin") === "USA")

  // use Spark SQL
  carsDF.createOrReplaceTempView("cars")  // with this, we can use "cars" view name in sql
  val americanCarsDF: DataFrame = spark.sql(
    """
      |select Name from cars where Origin = 'USA'
      |""".stripMargin)

  // we can run ANY SQL Statement
  // will create database rtjvm
  spark.sql("create database rtjvm")
  // from now on, SPARK SQL will use rtjvm db
  spark.sql("use rtjvm")
  val databasesDF: DataFrame = spark.sql("show databases")
  databasesDF.show()

  // how to transfer table from a DB to Spark tables

  val driver = "org.postgresql.Driver"
  val url = "jdbc:postgresql://localhost:5432/rtjvm"
  val user = "docker"
  val password = "docker"
  // this method will read table from postgres
  def readTable(dbtable: String) = spark.read
    .format("jdbc")
    .options(Map(
      "driver" -> driver,
      "url" -> url,
      "user" -> user,
      "password" -> password,
      "dbtable" -> dbtable))
    .load()

  def transferTables(tablesNames: List[String], shouldSaveToWarehouse: Boolean = false): Unit = tablesNames.foreach { tableName =>
    val tableDF = readTable(tableName)
    tableDF.createOrReplaceTempView(tableName)
    // below will save table from postgres to spark tables in warehouse folder under rtjvm.db
    if(shouldSaveToWarehouse) {
      tableDF.write
        .mode(SaveMode.Overwrite)
        .option("path", s"src/main/resources/warehouse/rtjvm.db/${tableName}")
        .saveAsTable(tableName)
    }
  }

  transferTables(List(
    "departments",
    "dept_emp",
    "dept_manager",
    "employees",
    "movies",
    "salaries",
    "titles"))

  // read DF from loaded spark table... will read from src/main/resources/warehouse/rtjvm.db which we populated above
//  val employeesDF2: DataFrame = spark.read.table("employees")
  // since now we have DF, we can operate on it

  /**
    * Exercises:
    * 1. Read the movies DF and store it in a Spark table in the rtjvm database.
    * 2. Count how many employees were hired in between Jan 1 1999 and Jan 1 2000
    * 3. Show the average salaries for the employees hired in between those dates, grouped by department.
    * 4. Show the name of the best paying department for employees hired in between those dates.
    */

  // Exercise 1: Read the movies DF and store it in a Spark table in the rtjvm database.
  val moviesDF = spark.read.json("src/main/resources/data/movies.json")
  /*moviesDF.write
    .mode(SaveMode.Overwrite)
    .option("path", "src/main/resources/warehouse/rtjvm.db/movies")
    .saveAsTable("movies")*/

  // Exercise 2: Count how many employees were hired in between Jan 1 1999 and Jan 1 2000

  spark.sql(
    """
      |select count(*) from employees
      |where hire_date > '1999-01-01' and hire_date < '2000-01-01'
      |""".stripMargin)
    .show()

  // Exercise 3: Show the average salaries for the employees hired in between those dates,
  // grouped by department.
  val avgSalaryPerDept: DataFrame = spark.sql(
    """
      |select d.dept_no, avg(s.salary) as avg_salary
      |from employees e, dept_emp d, salaries s
      |where hire_date > '1999-01-01' and hire_date < '2000-01-01'
      |and e.emp_no = d.emp_no
      |and e.emp_no = s.emp_no and s.from_date > '1999-01-01' and s.from_date < '2000-01-01'
      |group by d.dept_no
      |""".stripMargin)

  avgSalaryPerDept.createOrReplaceTempView("avgSalaryPerDept")
  avgSalaryPerDept.show()

  // Exercise 4: Show the name of the best paying department for employees hired in between those dates.
  val bestPayingDept = spark.sql(
    """
      |select d.dept_no, d.dept_name, a.avg_salary
      |from avgSalaryPerDept a, departments d
      |where a.dept_no = d.dept_no
      |order by a.avg_salary desc
      |limit 1
      |""".stripMargin)

  bestPayingDept.show()
}
