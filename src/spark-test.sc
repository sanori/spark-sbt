package example

import java.nio.file.Paths
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.log4j.{ Level, Logger }

object sparkTest {
  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)

  def fsPath(resource: String): String =
    Paths.get(this.getClass.getResource(resource).toURI).toString
  //> fsPath: (resource: String)String

  val spark = SparkSession.builder()
    .master("local[2]")
    .appName("scalaWorksheet")
    .config("spark.ui.showConsoleProgress", false)
    .getOrCreate() //> Using Spark's default log4j profile: org/apache/spark/log4j-defaults.propert
  //| ies
  //| spark  : org.apache.spark.sql.SparkSession = org.apache.spark.sql.SparkSessi
  //| on@1cfd1875

  import spark.implicits._

  val sc = spark.sparkContext //> sc  : org.apache.spark.SparkContext = org.apache.spark.SparkContext@28c0b664
  //| 
  val df = spark.read
    .format("csv")
    .option("sep", ";")
    .option("inferSchema", true)
    .load(fsPath("will_play_text.csv"))
    .toDF("line_id", "play_name", "speech_number", "line_number", "speaker", "text_entry")
  //> df  : org.apache.spark.sql.DataFrame = [line_id: int, play_name: string ... 
  //| 4 more fields]

  val words = df.select("text_entry")
    .flatMap(r => r(0).asInstanceOf[String].toLowerCase.split("\\s+"))
  //> words  : org.apache.spark.sql.Dataset[String] = [value: string]

  val rddSolution = words.rdd
    .map((_, 1))
    .reduceByKey(_ + _)
    .sortBy(-_._2)
    .take(10) //> rddSolution  : Array[(String, Int)] = Array((the,26992), (and,24250), (i,18
  //| 972), (to,18130), (of,15697), (a,13908), (my,11845), (in,10212), (you,10156
  //| ), (that,9757))

  val dfSolution = words.toDF("word")
    .groupBy("word")
    .count()
    .orderBy(col("count").desc)
    .take(10) //> dfSolution  : Array[org.apache.spark.sql.Row] = Array([the,26992], [and,242
  //| 50], [i,18972], [to,18130], [of,15697], [a,13908], [my,11845], [in,10212], 
  //| [you,10156], [that,9757])

  spark.stop()
}