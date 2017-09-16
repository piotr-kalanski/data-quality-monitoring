package com.datawizards.dqm.logger

import java.sql.DriverManager
import java.util.Properties

import com.datawizards.dqm.result._
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
        |   day INTEGER
        |);
        |
        |CREATE TABLE TABLE_STATISTICS(
        |   tableName VARCHAR,
        |   rowsCount INTEGER,
        |   columnsCount INTEGER,
        |   year INTEGER,
        |   month INTEGER,
        |   day INTEGER
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
        |   day INTEGER
        |);
        |
        |CREATE TABLE GROUP_STATISTICS(
        |   tableName VARCHAR,
        |   groupName VARCHAR,
        |   groupByFieldValue VARCHAR,
        |   rowsCount INTEGER,
        |   year INTEGER,
        |   month INTEGER,
        |   day INTEGER
        |);
        |
        |CREATE TABLE INVALID_GROUPS(
        |   tableName VARCHAR,
        |   groupName VARCHAR,
        |   groupValue VARCHAR,
        |   rule VARCHAR,
        |   year INTEGER,
        |   month INTEGER,
        |   day INTEGER
        |);
        |
        |CREATE TABLE INVALID_TABLE_TRENDS(
        |  tableName VARCHAR,
        |  rule VARCHAR,
        |  comment VARCHAR,
        |  year INTEGER,
        |  month INTEGER,
        |  day INTEGER
        |);
      """.stripMargin)
    val logger = new DatabaseValidationResultLogger(
      driverClassName = "org.h2.Driver",
      dbUrl = connectionString,
      connectionProperties = new Properties(),
      invalidRecordsTableName = "INVALID_RECORDS",
      tableStatisticsTableName = "TABLE_STATISTICS",
      columnStatisticsTableName = "COLUMN_STATISTICS",
      groupsStatisticsTableName = "GROUP_STATISTICS",
      invalidGroupsTableName = "INVALID_GROUPS",
      invalidTableTrendsTableName = "INVALID_TABLE_TRENDS"
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
        2
      )
    )
    val tableStatistics = TableStatistics(
      tableName = "t1",
      rowsCount = 5,
      columnsCount = 3,
      year = 2000,
      month = 1,
      day = 2
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
        day = 2
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
        day = 2
      )
    )
    val groupByStatisticsList = Seq(
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
    val invalidGroups = Seq(
      InvalidGroup(
        tableName = "table",
        groupName = "COUNTRY",
        groupValue = Some("country4"),
        rule = "NotEmptyGroup",
        year = 2000,
        month = 1,
        day = 2
      ),
      InvalidGroup(
        tableName = "table",
        groupName = "COUNTRY",
        groupValue = Some("country5"),
        rule = "NotEmptyGroup",
        year = 2000,
        month = 1,
        day = 2
      )
    )
    val invalidTableTrends = Seq(
      InvalidTableTrend(
        tableName = "table",
        rule = "rule1",
        comment = "comment",
        year = 2000,
        month = 1,
        day = 2
      ),
      InvalidTableTrend(
        tableName = "table",
        rule = "rule2",
        comment = "comment2",
        year = 2000,
        month = 1,
        day = 2
      )
    )
    logger.log(ValidationResult(
      invalidRecords = invalidRecords,
      tableStatistics = tableStatistics,
      columnsStatistics = columnsStatistics,
      groupByStatisticsList = groupByStatisticsList,
      invalidGroups = invalidGroups,
      invalidTableTrends = invalidTableTrends
    ))
    val resultInvalidRecords = selectTable[InvalidRecord](connection, "INVALID_RECORDS")._1
    val resultTableStatistics = selectTable[TableStatistics](connection, "TABLE_STATISTICS")._1
    val resultColumnStatistics = selectTable[ColumnStatistics](connection, "COLUMN_STATISTICS")._1
    val resultGroupByStatisticsList = selectTable[GroupByStatistics](connection, "GROUP_STATISTICS")._1
    val resultInvalidGroups = selectTable[InvalidGroup](connection, "INVALID_GROUPS")._1
    val resultInvalidTableTrend = selectTable[InvalidTableTrend](connection, "INVALID_TABLE_TRENDS")._1
    connection.close()

    resultInvalidRecords should equal(invalidRecords)
    resultTableStatistics should equal(Seq(tableStatistics))
    resultColumnStatistics should equal(columnsStatistics)
    resultGroupByStatisticsList should equal(groupByStatisticsList)
    resultInvalidGroups should equal(invalidGroups)
    resultInvalidTableTrend should equal(invalidTableTrends)
  }

}
