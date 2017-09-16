package com.datawizards.dqm

import java.sql.Date

import com.datawizards.dqm.configuration.loader.StaticConfigurationLoader
import com.datawizards.dqm.configuration.location.StaticTableLocation
import com.datawizards.dqm.configuration.{DataQualityMonitoringConfiguration, TableConfiguration}
import com.datawizards.dqm.mocks.{EmptyHistoryStatisticsReader, StaticAlertSender, StaticValidationResultLogger}
import com.datawizards.dqm.result.{ColumnStatistics, InvalidRecord, TableStatistics, ValidationResult}
import com.datawizards.dqm.rules.field.NotNullRule
import com.datawizards.dqm.rules.{FieldRules, TableRules}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{FunSuite, Matchers}

@RunWith(classOf[JUnitRunner])
class DataQualityMonitorTest extends FunSuite with Matchers {
  lazy val spark: SparkSession = SparkSession.builder().master("local").getOrCreate()

  test("DataQualityMonitorTest - simple") {
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

    val configurationLoader = new StaticConfigurationLoader(DataQualityMonitoringConfiguration(
      tablesConfiguration = Seq(
        TableConfiguration(
          location = StaticTableLocation(input, "table"),
          rules = TableRules(Seq(
            FieldRules(
              field = "f2",
              rules = Seq(
                NotNullRule
              )
            )
          )))
        )
      )
    )
    val logger = new StaticValidationResultLogger()
    val alertSender = new StaticAlertSender()
    val historyStatisticsReader = EmptyHistoryStatisticsReader
    val processingDate = Date.valueOf("2000-01-02")
    DataQualityMonitor.run(processingDate, configurationLoader, historyStatisticsReader, logger, alertSender)

    val expectedResult = ValidationResult(
      invalidRecords = Seq(
        InvalidRecord(
          tableName = "table",
          columnName = "f2",
          row = """{"f1" : "r2.f1", "f2" : "null", "f3" : "r2.f3"}""",
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
        columnsCount = 3,
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
        )
      )
    )
    logger.results.head should equal(expectedResult)
    alertSender.results.head should equal(expectedResult)
  }

}
