package com.datawizards.dqm.validator

import com.datawizards.dqm.configuration.location.StaticTableLocation
import com.datawizards.dqm.result.{ColumnStatistics, InvalidRecord, TableStatistics, ValidationResult}
import com.datawizards.dqm.rules.{FieldRules, NotNullRule, TableRules}
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
    val df = spark.createDataFrame(spark.sparkContext.parallelize(Seq(
      Row("r1.f1", "r1.f2", null),
      Row("r2.f1", null, "r2.f3"),
      Row(null, "r3.f2", "r3.f3")
    )), schema)
    val input = StaticTableLocation(df, "table")
    val result = DataValidator.validate(input, TableRules(Seq(
      FieldRules(
        field = "f2",
        rules = Seq(
          NotNullRule
        )
      )
    )))
    result should equal(ValidationResult(
      invalidRecords = Seq(
        InvalidRecord(
          tableName = "table",
          row = """{"f1" : "r2.f1", "f2" : "null", "f3" : "r2.f3"}""",
          value = "null",
          rule = "NOT NULL"
        )
      ),
      tableStatistics = TableStatistics(
        tableName = "table",
        rowsCount = 3,
        columnsCount = 3
      ),
      columnsStatistics = Seq(
        ColumnStatistics(
          tableName = "table",
          columnName = "f1",
          columnType = "StringType",
          notMissingCount = 2L,
          rowsCount = 3L,
          percentageNotMissing = 2.0/3.0
        ),
        ColumnStatistics(
          tableName = "table",
          columnName = "f2",
          columnType = "StringType",
          notMissingCount = 2L,
          rowsCount = 3L,
          percentageNotMissing = 2.0/3.0
        ),
        ColumnStatistics(
          tableName = "table",
          columnName = "f3",
          columnType = "StringType",
          notMissingCount = 2L,
          rowsCount = 3L,
          percentageNotMissing = 2.0/3.0
        )
      )
    ))
  }

}
