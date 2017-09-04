package com.datawizards.dqm.logger

import java.sql.DriverManager
import java.util.Properties

import com.datawizards.dqm.configuration.location.ColumnStatistics
import com.datawizards.dqm.result.{InvalidRecord, TableStatistics, ValidationResult}
import com.datawizards.jdbc2class._
import org.scalatest.{FunSuite, Matchers}

class DatabaseValidationResultLoggerTest extends FunSuite with Matchers {

  test("Database logger integration tests") {
    Class.forName("org.h2.Driver")
    val connectionString = "jdbc:h2:mem:test"
    val connection = DriverManager.getConnection(connectionString, "", "")
    connection.createStatement().execute(
      """CREATE TABLE INVALID_RECORDS(
        |   row VARCHAR,
        |   value VARCHAR,
        |   rule VARCHAR
        |);
        |
        |CREATE TABLE TABLE_STATISTICS(
        |   tableName VARCHAR,
        |   rowsCount INTEGER,
        |   columnsCount INTEGER
        |);
        |
        |CREATE TABLE COLUMN_STATISTICS(
        |   tableName VARCHAR,
        |   columnName VARCHAR,
        |   columnType VARCHAR,
        |   notMissingCount INTEGER,
        |   rowsCount INTEGER,
        |   percentageNotMissing DOUBLE,
        |   min DOUBLE,
        |   max DOUBLE,
        |   avg DOUBLE,
        |   stddev DOUBLE
        |);
      """.stripMargin)
    val logger = new DatabaseValidationResultLogger(
      driverClassName = "org.h2.Driver",
      dbUrl = connectionString,
      connectionProperties = new Properties(),
      invalidRecordsTableName = "INVALID_RECORDS",
      tableStatisticsTableName = "TABLE_STATISTICS",
      columnStatisticsTableName = "COLUMN_STATISTICS"
    )
    val invalidRecords = Seq(
      InvalidRecord("{c:value}", "value", "NOT NULL")
    )
    val tableStatistics = TableStatistics(
      tableName = "t1",
      rowsCount = 5,
      columnsCount = 3
    )
    val columnsStatistics = Seq(
      ColumnStatistics(
        tableName = "t1",
        columnName = "c1",
        columnType = "StringType",
        notMissingCount = 10,
        rowsCount = 20,
        percentageNotMissing = 50.0
      ),
      ColumnStatistics(
        tableName = "t1",
        columnName = "c2",
        columnType = "IntType",
        notMissingCount = 30,
        rowsCount = 50,
        percentageNotMissing = 60.0
      )
    )
    logger.log(ValidationResult(
      invalidRecords = invalidRecords,
      tableStatistics = tableStatistics,
      columnsStatistics = columnsStatistics
    ))
    val resultInvalidRecords = selectTable[InvalidRecord](connection, "INVALID_RECORDS")._1
    val resultTableStatistics = selectTable[TableStatistics](connection, "TABLE_STATISTICS")._1
    val resultColumnStatistics = selectTable[ColumnStatistics](connection, "COLUMN_STATISTICS")._1
    connection.close()

    resultInvalidRecords should equal(invalidRecords)
    resultTableStatistics should equal(Seq(tableStatistics))
    resultColumnStatistics should equal(columnsStatistics)
  }

}
