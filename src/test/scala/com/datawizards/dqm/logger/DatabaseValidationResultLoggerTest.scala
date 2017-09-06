package com.datawizards.dqm.logger

import java.sql.{Date, DriverManager}
import java.util.Properties

import com.datawizards.dqm.result.{ColumnStatistics, InvalidRecord, TableStatistics, ValidationResult}
import com.datawizards.jdbc2class._
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{FunSuite, Matchers}

@RunWith(classOf[JUnitRunner])
class DatabaseValidationResultLoggerTest extends FunSuite with Matchers {

  test("Database logger integration tests") {
    Class.forName("org.h2.Driver")
    val connectionString = "jdbc:h2:mem:test"
    val connection = DriverManager.getConnection(connectionString, "", "")
    connection.createStatement().execute(
      """CREATE TABLE INVALID_RECORDS(
        |   tableName VARCHAR,
        |   columnName VARCHAR,
        |   row VARCHAR,
        |   value VARCHAR,
        |   rule VARCHAR,
        |   year INTEGER,
        |   month INTEGER,
        |   day INTEGER,
        |   date DATE
        |);
        |
        |CREATE TABLE TABLE_STATISTICS(
        |   tableName VARCHAR,
        |   rowsCount INTEGER,
        |   columnsCount INTEGER,
        |   year INTEGER,
        |   month INTEGER,
        |   day INTEGER,
        |   date DATE
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
        |   stddev DOUBLE,
        |   year INTEGER,
        |   month INTEGER,
        |   day INTEGER,
        |   date DATE
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
      InvalidRecord(
        "table",
        "c",
        "{c:value}",
        "value",
        "NOT NULL",
        2000,
        1,
        2,
        Date.valueOf("2000-01-02")
      )
    )
    val tableStatistics = TableStatistics(
      tableName = "t1",
      rowsCount = 5,
      columnsCount = 3,
      year = 2000,
      month = 1,
      day = 2,
      date = Date.valueOf("2000-01-02")
    )
    val columnsStatistics = Seq(
      ColumnStatistics(
        tableName = "t1",
        columnName = "c1",
        columnType = "StringType",
        notMissingCount = 10,
        rowsCount = 20,
        percentageNotMissing = 50.0,
        year = 2000,
        month = 1,
        day = 2,
        date = Date.valueOf("2000-01-02")
      ),
      ColumnStatistics(
        tableName = "t1",
        columnName = "c2",
        columnType = "IntType",
        notMissingCount = 30,
        rowsCount = 50,
        percentageNotMissing = 60.0,
        year = 2000,
        month = 1,
        day = 2,
        date = Date.valueOf("2000-01-02")
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
