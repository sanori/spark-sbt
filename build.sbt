import Dependencies._

lazy val root = (project in file(".")).
  settings(
    inThisBuild(List(
      organization := "com.example",
      scalaVersion := "2.12.12",
      version      := "0.1.0-SNAPSHOT"
    )),
    name := "SparkExample",
    libraryDependencies += "org.apache.spark" %% "spark-core" % "3.0.0",
    libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.0.0",
    libraryDependencies += scalaTest % Test
  )

initialCommands in console := """
  import org.apache.log4j.{Level, Logger}
  import org.apache.spark.sql.SparkSession
  import org.apache.spark.sql.functions._
  Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
  val spark = SparkSession.builder
    .master("local[*]")
    .appName("spark-shell")
    .getOrCreate
  import spark.implicits._
  lazy val sc = spark.sparkContext
"""

cleanupCommands in console := "if (spark != null) spark.stop()"
