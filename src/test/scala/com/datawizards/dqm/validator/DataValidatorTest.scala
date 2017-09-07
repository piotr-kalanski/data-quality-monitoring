package com.datawizards.dqm.validator

import java.sql.Date

import com.datawizards.dqm.configuration.{GroupByConfiguration, TableConfiguration}
import com.datawizards.dqm.configuration.location.StaticTableLocation
import com.datawizards.dqm.filter.FilterByYearMonthDayColumns
import com.datawizards.dqm.result._
import com.datawizards.dqm.rules.{FieldRules, NotEmptyGroups, NotNullRule, TableRules}
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
          day = 2
        )
      ),
      tableStatistics = TableStatistics(
        tableName = "table",
        rowsCount = 3,
        columnsCount = 6,
        year = 2000,
        month = 1,
        day = 2
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
          day = 2
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
          day = 2
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
          day = 2
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
          day = 2
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
          day = 2
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
          day = 2
        )
      )
    ))
  }

  test("Validate records - group statistics") {
    val schema = StructType(Seq(
      StructField("country", StringType)
    ))
    val df = spark.createDataFrame(spark.sparkContext.parallelize(Seq(
      Row("country1"),
      Row("country1"),
      Row("country1"),
      Row("country2"),
      Row("country2"),
      Row("country3")
    )), schema)
    val processingDate = Date.valueOf("2000-01-02")
    val input = StaticTableLocation(df, "table")
    val result = DataValidator.validate(TableConfiguration(
      location = input,
      rules = TableRules(Seq.empty),
      groups = Seq(GroupByConfiguration("COUNTRY", "country"))
    ), processingDate)
    result.copy(groupByStatisticsList = result.groupByStatisticsList.sortBy(_.groupByFieldValue)) should equal(ValidationResult(
      invalidRecords = Seq.empty,
      tableStatistics = TableStatistics(
        tableName = "table",
        rowsCount = 6,
        columnsCount = 1,
        year = 2000,
        month = 1,
        day = 2
      ),
      columnsStatistics = Seq(
        ColumnStatistics(
          tableName = "table",
          columnName = "country",
          columnType = "StringType",
          notMissingCount = 6L,
          rowsCount = 6L,
          percentageNotMissing = 6.0/6.0,
          year = 2000,
          month = 1,
          day = 2
        )
      ),
      groupByStatisticsList = Seq(
        GroupByStatistics(
          tableName = "table",
          groupName = "COUNTRY",
          groupByFieldValue = "country1",
          rowsCount = 3,
          year = 2000,
          month = 1,
          day = 2
        ),
        GroupByStatistics(
          tableName = "table",
          groupName = "COUNTRY",
          groupByFieldValue = "country2",
          rowsCount = 2,
          year = 2000,
          month = 1,
          day = 2
        ),
        GroupByStatistics(
          tableName = "table",
          groupName = "COUNTRY",
          groupByFieldValue = "country3",
          rowsCount = 1,
          year = 2000,
          month = 1,
          day = 2
        )
      )
    ))
  }

  test("Validate records - group validation rules") {
    val schema = StructType(Seq(
      StructField("country", StringType)
    ))
    val df = spark.createDataFrame(spark.sparkContext.parallelize(Seq(
      Row("country1"),
      Row("country1"),
      Row("country1"),
      Row("country2"),
      Row("country2")
    )), schema)
    val processingDate = Date.valueOf("2000-01-02")
    val input = StaticTableLocation(df, "table")
    val result = DataValidator.validate(TableConfiguration(
      location = input,
      rules = TableRules(Seq.empty),
      groups = Seq(GroupByConfiguration("COUNTRY", "country", Seq(NotEmptyGroups(Seq("country1","country2","country3")))))
    ), processingDate)
    result.copy(groupByStatisticsList = result.groupByStatisticsList.sortBy(_.groupByFieldValue)) should equal(ValidationResult(
      invalidRecords = Seq.empty,
      tableStatistics = TableStatistics(
        tableName = "table",
        rowsCount = 5,
        columnsCount = 1,
        year = 2000,
        month = 1,
        day = 2
      ),
      columnsStatistics = Seq(
        ColumnStatistics(
          tableName = "table",
          columnName = "country",
          columnType = "StringType",
          notMissingCount = 5L,
          rowsCount = 5L,
          percentageNotMissing = 5.0/5.0,
          year = 2000,
          month = 1,
          day = 2
        )
      ),
      groupByStatisticsList = Seq(
        GroupByStatistics(
          tableName = "table",
          groupName = "COUNTRY",
          groupByFieldValue = "country1",
          rowsCount = 3,
          year = 2000,
          month = 1,
          day = 2
        ),
        GroupByStatistics(
          tableName = "table",
          groupName = "COUNTRY",
          groupByFieldValue = "country2",
          rowsCount = 2,
          year = 2000,
          month = 1,
          day = 2
        )
      ),
      invalidGroups = Seq(
        InvalidGroup(
          tableName = "table",
          groupName = "COUNTRY",
          rule = "NotEmptyGroups",
          year = 2000,
          month = 1,
          day = 2
        )
      )
    ))

  }

}
