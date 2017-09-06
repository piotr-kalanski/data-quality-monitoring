package com.datawizards.dqm.validator

import java.sql.Date
import com.datawizards.dqm.configuration.TableConfiguration
import com.datawizards.dqm.configuration.location.StaticTableLocation
import com.datawizards.dqm.filter.FilterByYearMonthDayColumns
import com.datawizards.dqm.result.{ColumnStatistics, InvalidRecord, TableStatistics, ValidationResult}
import com.datawizards.dqm.rules.{FieldRules, NotNullRule, TableRules}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{FunSuite, Matchers}

@RunWith(classOf[JUnitRunner])
class DataValidatorTest extends FunSuite with Matchers {
  lazy val spark: SparkSession = SparkSession.builder().master("local").getOrCreate()

  test("Validate records - simple") {
    val schema = StructType(Seq(
      StructField("f1", StringType),
      StructField("f2", StringType),
      StructField("f3", StringType),
      StructField("year", IntegerType),
      StructField("month", IntegerType),
      StructField("day", IntegerType)
    ))
    val df = spark.createDataFrame(spark.sparkContext.parallelize(Seq(
      Row("r1.f1", "r1.f2", null, 2000, 1, 2), // processing date
      Row("r2.f1", null, "r2.f3", 2000, 1, 2), // processing date
      Row(null, "r3.f2", "r3.f3", 2000, 1, 2), // processing date
      Row(null, "r3.f2", "r3.f3", 2000, 1, 3) // not processing date
    )), schema)
    val processingDate = Date.valueOf("2000-01-02")
    val input = StaticTableLocation(df, "table")
    val result = DataValidator.validate(TableConfiguration(
      input,
      TableRules(Seq(
        FieldRules(
          field = "f2",
          rules = Seq(
            NotNullRule
          )
        ))),
      Some(FilterByYearMonthDayColumns)
    ), processingDate)
    result should equal(ValidationResult(
      invalidRecords = Seq(
        InvalidRecord(
          tableName = "table",
          columnName = "f2",
          row = """{"f1" : "r2.f1", "year" : 2000, "f3" : "r2.f3", "day" : 2, "month" : 1, "f2" : "null"}""",
          value = "null",
          rule = "NOT NULL",
          year = 2000,
          month = 1,
          day = 2,
          date = processingDate
        )
      ),
      tableStatistics = TableStatistics(
        tableName = "table",
        rowsCount = 3,
        columnsCount = 6,
        year = 2000,
        month = 1,
        day = 2,
        date = processingDate
      ),
      columnsStatistics = Seq(
        ColumnStatistics(
          tableName = "table",
          columnName = "f1",
          columnType = "StringType",
          notMissingCount = 2L,
          rowsCount = 3L,
          percentageNotMissing = 2.0/3.0,
          year = 2000,
          month = 1,
          day = 2,
          date = processingDate
        ),
        ColumnStatistics(
          tableName = "table",
          columnName = "f2",
          columnType = "StringType",
          notMissingCount = 2L,
          rowsCount = 3L,
          percentageNotMissing = 2.0/3.0,
          year = 2000,
          month = 1,
          day = 2,
          date = processingDate
        ),
        ColumnStatistics(
          tableName = "table",
          columnName = "f3",
          columnType = "StringType",
          notMissingCount = 2L,
          rowsCount = 3L,
          percentageNotMissing = 2.0/3.0,
          year = 2000,
          month = 1,
          day = 2,
          date = processingDate
        ),
        ColumnStatistics(
          tableName = "table",
          columnName = "year",
          columnType = "IntegerType",
          notMissingCount = 3L,
          rowsCount = 3L,
          percentageNotMissing = 3.0/3.0,
          min = Some(2000.0),
          max = Some(2000.0),
          avg = Some(2000.0),
          stddev = Some(0.0),
          year = 2000,
          month = 1,
          day = 2,
          date = processingDate
        ),
        ColumnStatistics(
          tableName = "table",
          columnName = "month",
          columnType = "IntegerType",
          notMissingCount = 3L,
          rowsCount = 3L,
          percentageNotMissing = 3.0/3.0,
          min = Some(1.0),
          max = Some(1.0),
          avg = Some(1.0),
          stddev = Some(0.0),
          year = 2000,
          month = 1,
          day = 2,
          date = processingDate
        ),
        ColumnStatistics(
          tableName = "table",
          columnName = "day",
          columnType = "IntegerType",
          notMissingCount = 3L,
          rowsCount = 3L,
          percentageNotMissing = 3.0/3.0,
          min = Some(2.0),
          max = Some(2.0),
          avg = Some(2.0),
          stddev = Some(0.0),
          year = 2000,
          month = 1,
          day = 2,
          date = processingDate
        )
      )
    ))
  }

}
