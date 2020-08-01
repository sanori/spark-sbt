package example

import org.scalatest._
import funspec.AnyFunSpec
import org.apache.spark.sql.SparkSession

class ShakespeareSpec extends AnyFunSpec with BeforeAndAfter {
  private var spark: SparkSession = _
  before {
    spark = SparkSession.builder
      .master("local")
      .appName("ShakespeareSpec")
      .getOrCreate
  }

  after {
    if (spark != null) spark.stop()
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
