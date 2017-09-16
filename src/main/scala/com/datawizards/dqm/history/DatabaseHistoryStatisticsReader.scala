package com.datawizards.dqm.history

import java.sql.DriverManager
import java.util.Properties

import com.datawizards.dqm.result.TableStatistics

import scala.collection.mutable.ListBuffer

/**
  * Reads historical table statistics from DB
  * <br/>
  * Expected DB schema for tableStatisticsTableName:
  * <pre>
  *   CREATE TABLE TABLE_STATISTICS(
  *     tableName VARCHAR,
  *     rowsCount INTEGER,
  *     columnsCount INTEGER,
  *     year INTEGER,
  *     month INTEGER,
  *     day INTEGER
  *   )
  * </pre>
  *
  * @param driverClassName JDBC driver class name
  * @param dbUrl DB connection string
  * @param connectionProperties JDBC connection properties, especially user and password
  * @param tableStatisticsTableName name of table where to insert table statistics records
  */
class DatabaseHistoryStatisticsReader(
                                       driverClassName: String,
                                       dbUrl: String,
                                       connectionProperties: Properties,
                                       tableStatisticsTableName: String
                                     ) extends HistoryStatisticsReader {

  override def readTableStatistics(tableName: String): Seq[TableStatistics] = {
    Class.forName(driverClassName)
    val connection = DriverManager.getConnection(dbUrl, connectionProperties)
    val rs = connection.createStatement().executeQuery(s"SELECT * FROM $tableStatisticsTableName WHERE tableName = '$tableName'")
    val result = new ListBuffer[TableStatistics]
    while(rs.next()) {
      result += TableStatistics(
        tableName = rs.getString("tableName"),
        rowsCount = rs.getLong("rowsCount"),
        columnsCount = rs.getInt("columnsCount"),
        year = rs.getInt("year"),
        month = rs.getInt("month"),
        day = rs.getInt("day")
      )
    }
    connection.close()
    result.toList
  }

}
