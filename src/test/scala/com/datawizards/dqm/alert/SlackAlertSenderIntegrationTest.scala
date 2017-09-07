package com.datawizards.dqm.alert

import java.sql.Date
import com.datawizards.dqm.result.{ColumnStatistics, InvalidRecord, TableStatistics, ValidationResult}

object SlackAlertSenderIntegrationTest extends App {
  val alertSender = new SlackAlertSender("https://hooks.slack.com/services/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", "workray-monitoring", "Data Quality Monitoring")
  val processingDate = Date.valueOf("2000-01-02")
  val result = ValidationResult(
    invalidRecords = Seq(
      InvalidRecord(
        tableName = "CLIENT",
        columnName = "12",
        row = """{"f1" : "r2.f1", "year" : 2000, "f3" : "r2.f3", "day" : 2, "month" : 1, "f2" : "null"}""",
        value = "null",
        rule = "NOT NULL",
        year = 2000,
        month = 1,
        day = 2
      ),
      InvalidRecord(
        tableName = "CLIENT",
        columnName = "f2",
        row = """{"f1" : "r2.f1", "year" : 2000, "f3" : "r2.f3", "day" : 2, "month" : 1, "f2" : "-1"}""",
        value = "null",
        rule = "min",
        year = 2000,
        month = 1,
        day = 2
      )
    ),
    tableStatistics = TableStatistics(
      tableName = "CLIENT",
      rowsCount = 0,
      columnsCount = 6,
      year = 2000,
      month = 1,
      day = 2
    ),
    columnsStatistics = Seq(
      ColumnStatistics(
        tableName = "CLIENT",
        columnName = "f1",
        columnType = "StringType",
        notMissingCount = 0L,
        rowsCount = 3L,
        percentageNotMissing = 0.0/3.0,
        year = 2000,
        month = 1,
        day = 2
      ),
      ColumnStatistics(
        tableName = "CLIENT",
        columnName = "f2",
        columnType = "StringType",
        notMissingCount = 0L,
        rowsCount = 3L,
        percentageNotMissing = 0.0/3.0,
        year = 2000,
        month = 1,
        day = 2
      ),
      ColumnStatistics(
        tableName = "CLIENT",
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
        tableName = "CLIENT",
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
        tableName = "CLIENT",
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
        tableName = "CLIENT",
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
  )

  alertSender.send(result)
}
