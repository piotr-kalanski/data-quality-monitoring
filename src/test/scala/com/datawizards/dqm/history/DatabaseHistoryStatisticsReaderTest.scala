package com.datawizards.dqm.history

import java.sql.DriverManager
import java.util.Properties

import com.datawizards.dqm.result.TableStatistics
import org.junit.runner.RunWith
import org.scalatest.{FunSuite, Matchers}
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class DatabaseHistoryStatisticsReaderTest extends FunSuite with Matchers {

  test("Read table statistics from DB") {
    Class.forName("org.h2.Driver")
    val connectionString = "jdbc:h2:mem:test"
    val connection = DriverManager.getConnection(connectionString, "", "")
    connection.createStatement().execute(
      """CREATE TABLE TABLE_STATISTICS_HISTORY_TEST(
        |   tableName VARCHAR,
        |   rowsCount INTEGER,
        |   columnsCount INTEGER,
        |   year INTEGER,
        |   month INTEGER,
        |   day INTEGER
        |);
        |
        |INSERT INTO TABLE_STATISTICS_HISTORY_TEST VALUES('table1',10,5,2000,1,2);
        |INSERT INTO TABLE_STATISTICS_HISTORY_TEST VALUES('table1',12,5,2000,1,3);
        |INSERT INTO TABLE_STATISTICS_HISTORY_TEST VALUES('table1',14,5,2000,1,4);
        |
        |INSERT INTO TABLE_STATISTICS_HISTORY_TEST VALUES('table2',232,15,2000,1,2);
        |INSERT INTO TABLE_STATISTICS_HISTORY_TEST VALUES('table2',1355,15,2000,1,3);
      """.stripMargin)

    val historyStatisticsReader = new DatabaseHistoryStatisticsReader(
      driverClassName = "org.h2.Driver",
      dbUrl = connectionString,
      connectionProperties = new Properties(),
      tableStatisticsTableName = "TABLE_STATISTICS_HISTORY_TEST"
    )

    val table1StatisticsList = Seq(
      TableStatistics(
        tableName = "table1",
        rowsCount = 10L,
        columnsCount = 5,
        year = 2000,
        month = 1,
        day = 2
      ),
      TableStatistics(
        tableName = "table1",
        rowsCount = 12L,
        columnsCount = 5,
        year = 2000,
        month = 1,
        day = 3
      ),
      TableStatistics(
        tableName = "table1",
        rowsCount = 14L,
        columnsCount = 5,
        year = 2000,
        month = 1,
        day = 4
      )
    )
    val table2StatisticsList = Seq(
      TableStatistics(
        tableName = "table2",
        rowsCount = 232L,
        columnsCount = 15,
        year = 2000,
        month = 1,
        day = 2
      ),
      TableStatistics(
        tableName = "table2",
        rowsCount = 1355L,
        columnsCount = 15,
        year = 2000,
        month = 1,
        day = 3
      )
    )

    historyStatisticsReader.readTableStatistics("table1") should equal(table1StatisticsList)
    historyStatisticsReader.readTableStatistics("table2") should equal(table2StatisticsList)

    connection.close()
  }

}
