package example

import java.nio.file.Paths
import org.apache.spark.sql.{ SparkSession, DataFrame }
import org.apache.spark.sql.functions._

object Shakespeare {
  def fsPath(resource: String): String =
    Paths.get(this.getClass.getResource(resource).toURI).toString

  def df(spark: SparkSession): DataFrame = spark.read
    .format("csv")
    .option("sep", ";")
    .option("inferSchema", true)
    .load(fsPath("will_play_text.csv"))
    .toDF("line_id", "play_name", "speech_number", "line_number", "speaker", "text_entry")

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .appName("Shakespeare")
      .getOrCreate()
    import spark.implicits._
    // val sc = spark.sparkContext
    df(spark)
    spark.stop()
  }
}
