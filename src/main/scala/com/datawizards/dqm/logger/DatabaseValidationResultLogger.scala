package com.datawizards.dqm.logger

import java.sql.DriverManager
import java.util.Properties

import com.datawizards.dqm.result.{ColumnStatistics, GroupByStatistics, InvalidRecord, TableStatistics}
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
  * @param driverClassName JDBC driver class name
  * @param dbUrl DB connection string
  * @param connectionProperties JDBC connection properties, especially user and password
  * @param invalidRecordsTableName name of table where to insert invalid records
  * @param tableStatisticsTableName name of table where to insert table statistics records
  * @param columnStatisticsTableName name of table where to insert column statistics records
  * @param groupsStatisticsTableName name of table where to insert group by statistics records
  */
class DatabaseValidationResultLogger(
                                      driverClassName: String,
                                      dbUrl: String,
                                      connectionProperties: Properties,
                                      invalidRecordsTableName: String,
                                      tableStatisticsTableName: String,
                                      columnStatisticsTableName: String,
                                      groupsStatisticsTableName: String
                                    ) extends ValidationResultLogger {

  override protected def logInvalidRecords(invalidRecords: Seq[InvalidRecord]): Unit = {
    executeInserts(generateInserts(invalidRecords, invalidRecordsTableName))
  }

  override protected def logTableStatistics(tableStatistics: TableStatistics): Unit = {
    executeInserts(generateInserts(Seq(tableStatistics), tableStatisticsTableName))
  }

  override protected def logColumnStatistics(columnsStatistics: Seq[ColumnStatistics]): Unit = {
    executeInserts(generateInserts(columnsStatistics, columnStatisticsTableName))
  }

  override protected def logGroupByStatistics(groupByStatisticsList: Seq[GroupByStatistics]): Unit = {
    executeInserts(generateInserts(groupByStatisticsList, groupsStatisticsTableName))
  }

  private def executeInserts(inserts: Traversable[String]): Unit = {
    Class.forName(driverClassName)
    val connection = DriverManager.getConnection(dbUrl, connectionProperties)
    connection.createStatement().execute(inserts.mkString(";"))
    connection.close()
  }

}
