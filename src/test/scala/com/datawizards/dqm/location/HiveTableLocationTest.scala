package com.datawizards.dqm.location

import com.datawizards.dqm.configuration.location.HiveTableLocation
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}
import org.junit.runner.RunWith
import org.scalatest.{FunSuite, Matchers}
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class HiveTableLocationTest extends FunSuite with Matchers {
  lazy val spark: SparkSession = SparkSession.builder().master("local").getOrCreate()

  test("Load hive table") {
    val schema = StructType(Seq(
      StructField("f1", StringType),
      StructField("f2", StringType),
      StructField("f3", StringType)
    ))
    val input = spark.createDataFrame(spark.sparkContext.parallelize(Seq(
      Row("r1.f1", "r1.f2", null),
      Row("r2.f1", null, "r2.f3"),
      Row(null, "r3.f2", "r3.f3")
    )), schema)

    val table = "test"
    input.write.mode("overwrite").saveAsTable(table)

    val location = HiveTableLocation(table)
    location.load().collect() should equal(input.collect())
  }

}
