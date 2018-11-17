import Dependencies._

lazy val root = (project in file(".")).
  settings(
    inThisBuild(List(
      organization := "com.example",
      scalaVersion := "2.12.7",
      version      := "0.1.0-SNAPSHOT"
    )),
    name := "SparkExample",
    libraryDependencies += "org.apache.spark" %% "spark-core" % "2.4.0",
    libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.4.0",
    libraryDependencies += scalaTest % Test
  )

initialCommands in console := """
  import org.apache.spark.sql.SparkSession
  import org.apache.spark.sql.functions._
  val spark = SparkSession.builder()
    .master("local")
    .appName("spark-shell")
    .getOrCreate()
  import spark.implicits._
  val sc = spark.sparkContext
"""

cleanupCommands in console := "spark.stop()"
