package com.datawizards.dqm.logger

import java.sql.DriverManager
import java.util.Properties

import com.datawizards.dqm.result._
import com.datawizards.class2jdbc._

/**
  * Validation results logger saving result in RDBMS
  * <br/>
  * Expected DB schema for invalidRecordsTableName:
  * <pre>
  *   CREATE TABLE INVALID_RECORDS(
  *     tableName VARCHAR,
  *     columnName VARCHAR,
  *     row VARCHAR,
  *     value VARCHAR,
  *     rule VARCHAR,
  *     year INTEGER,
  *     month INTEGER,
  *     day INTEGER
  *   )
  * </pre>
  *
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
  * Expected DB schema for columnStatisticsTableName:
  * <pre>
  *   CREATE TABLE COLUMN_STATISTICS(
  *     tableName VARCHAR,
  *     columnName VARCHAR,
  *     columnType VARCHAR,
  *     notMissingCount INTEGER,
  *     rowsCount INTEGER,
  *     percentageNotMissing DOUBLE,
  *     min DOUBLE,
  *     max DOUBLE,
  *     avg DOUBLE,
  *     stddev DOUBLE,
  *     year INTEGER,
  *     month INTEGER,
  *     day INTEGER
  *   )
  * </pre>
  *
  * Expected DB schema for groupsStatisticsTableName:
  * <pre>
  *   CREATE TABLE GROUP_STATISTICS(
  *     tableName VARCHAR,
  *     groupName VARCHAR,
  *     groupByFieldValue VARCHAR,
  *     rowsCount INTEGER,
  *     year INTEGER,
  *     month INTEGER,
  *     day INTEGER
  *   )
  * </pre>
  *
  * Expected DB schema for invalidGroupsTableName:
  * <pre>
  *   CREATE TABLE INVALID_GROUPS(
  *     tableName VARCHAR,
  *     groupName VARCHAR,
  *     groupValue VARCHAR,
  *     rule VARCHAR,
  *     year INTEGER,
  *     month INTEGER,
  *     day INTEGER
  *   )
  * </pre>
  *
  * Expected DB schema for invalidTableTrendsTableName:
  * <pre>
  *   CREATE TABLE INVALID_TABLE_TRENDS(
  *     tableName VARCHAR,
  *     rule VARCHAR,
  *     comment VARCHAR,
  *     year INTEGER,
  *     month INTEGER,
  *     day INTEGER
  *   )
  * </pre>
  *
  * @param driverClassName JDBC driver class name
  * @param dbUrl DB connection string
  * @param connectionProperties JDBC connection properties, especially user and password
  * @param invalidRecordsTableName name of table where to insert invalid records
  * @param tableStatisticsTableName name of table where to insert table statistics records
  * @param columnStatisticsTableName name of table where to insert column statistics records
  * @param groupsStatisticsTableName name of table where to insert group by statistics records
  * @param invalidGroupsTableName name of table where to insert invalid groups
  * @param invalidTableTrendsTableName name of table where to insert invalid table trends
  */
class DatabaseValidationResultLogger(
                                      driverClassName: String,
                                      dbUrl: String,
                                      connectionProperties: Properties,
                                      invalidRecordsTableName: String,
                                      tableStatisticsTableName: String,
                                      columnStatisticsTableName: String,
                                      groupsStatisticsTableName: String,
                                      invalidGroupsTableName: String,
                                      invalidTableTrendsTableName: String
                                    ) extends ValidationResultLogger {

  override protected def logInvalidRecords(invalidRecords: Seq[InvalidRecord]): Unit = {
    executeStatements(generateInserts(invalidRecords, invalidRecordsTableName))
  }

  override protected def logTableStatistics(tableStatistics: TableStatistics): Unit = {
    val tableStatisticsList = Seq(tableStatistics)
    executeStatements(
      generateDeletes(tableStatisticsList, tableStatisticsTableName){
        t => s"tableName = '${t.tableName}' AND year = ${t.year} AND month = ${t.month} AND day = ${t.day}"
      } ++ generateInserts(tableStatisticsList, tableStatisticsTableName)
    )
  }

  override protected def logColumnStatistics(columnsStatistics: Seq[ColumnStatistics]): Unit = {
    executeStatements(
      generateDeletes(columnsStatistics, columnStatisticsTableName){
        t => s"tableName = '${t.tableName}' AND columnName = '${t.columnName}' AND year = ${t.year} AND month = ${t.month} AND day = ${t.day}"
      } ++ generateInserts(columnsStatistics, columnStatisticsTableName)
    )
  }

  override protected def logGroupByStatistics(groupByStatisticsList: Seq[GroupByStatistics]): Unit = {
    executeStatements(generateInserts(groupByStatisticsList, groupsStatisticsTableName))
  }

  override protected def logInvalidGroups(invalidGroups: Seq[InvalidGroup]): Unit = {
    executeStatements(generateInserts(invalidGroups, invalidGroupsTableName))
  }

  override protected def logInvalidTableTrends(invalidTableTrends: Seq[InvalidTableTrend]): Unit = {
    executeStatements(generateInserts(invalidTableTrends, invalidTableTrendsTableName))
  }

  private def executeStatements(statements: Traversable[String]): Unit = {
    Class.forName(driverClassName)
    val connection = DriverManager.getConnection(dbUrl, connectionProperties)
    for(statement <- statements)
      connection.createStatement().execute(statement)
    connection.close()
  }

  private def generateDeletes[T](data: Seq[T], table: String)(whereCondition: T => String): Traversable[String] = {
    data.map(d => s"DELETE FROM $table WHERE ${whereCondition(d)}")
  }

}
