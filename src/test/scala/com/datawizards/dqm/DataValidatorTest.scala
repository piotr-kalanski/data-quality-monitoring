package com.datawizards.dqm

import com.datawizards.dqm.model.{FieldRules, InvalidRecord, ValidationResult}
import com.datawizards.dqm.rules.NotNullRule
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}
import org.scalatest.{FunSuite, Matchers}

class DataValidatorTest extends FunSuite with Matchers {
  lazy val spark: SparkSession = SparkSession.builder().master("local").getOrCreate()

  test("Validate records - simple") {
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
    val result = DataValidator.validate(input, Seq(
      FieldRules(
        field = "f2",
        rules = Seq(
          new NotNullRule()
        )
      )
    ))
    result should equal(ValidationResult(
      invalidRecords = Seq(
        InvalidRecord(
          row = """{"f1" : "r2.f1", "f2" : "null", "f3" : "r2.f3"}""",
          value = "null",
          rule = "NOT NULL"
        )
      )
    ))
  }

}
