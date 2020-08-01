package example

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.scalatest._
import funspec.AnyFunSpec

class ShakespeareSpec extends AnyFunSpec with BeforeAndAfterAll {
  lazy val spark: SparkSession = SparkSession.builder
    .master("local[*]")
    .appName(this.getClass.getName)
    .getOrCreate

  override def beforeAll(): Unit = {
    super.beforeAll()
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    spark
  }

  override def afterAll(): Unit = {
    if (spark != null) spark.stop()
    super.afterAll()
  }

  describe("Shakespeare.df") {
    it("should return a non-empty DataFrame with 6 columns") {
      val df = Shakespeare.df(spark)
      assert(df.count() > 1)
      assert(df.columns.length === 6)
      assert(df.columns(5) === "text_entry")
    }
  }
}
