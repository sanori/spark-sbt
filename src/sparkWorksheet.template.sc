import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.log4j.{ Level, Logger }

object sparkWorksheet {
  // Set off the vebose log messages
  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)

  val spark = SparkSession.builder
    .master("local")
    .appName("sparkWorksheet")
    .config("spark.ui.showConsoleProgress", false)
    .getOrCreate()

  import spark.implicits._

  lazy val sc = spark.sparkContext

  // Your Spark codes are here
  // spark: sparkSession object (like sqlContext in Spark 1.x)
  // sc: sparkContext object (Spark 1.x compatible)

  spark.stop()
}
