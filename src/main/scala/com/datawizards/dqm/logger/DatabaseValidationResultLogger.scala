package com.datawizards.dqm.logger

import java.sql.DriverManager
import java.util.Properties

import com.datawizards.dqm.result.InvalidRecord
import com.datawizards.class2jdbc._

/**
  * Expected DB schema for invalidRecordsTableName:
  *
  * <pre>
  *   CREATE TABLE INVALID_RECORDS(
  *     row VARCHAR,
  *     value VARCHAR,
  *     rule VARCHAR
  *   )
  * </pre>
  *
  * @param driverClassName
  * @param dbUrl
  * @param connectionProperties
  * @param invalidRecordsTableName
  */
class DatabaseValidationResultLogger(driverClassName: String, dbUrl: String, connectionProperties: Properties, invalidRecordsTableName: String) extends ValidationResultLogger {

  override protected def logInvalidRecords(invalidRecords: Seq[InvalidRecord]): Unit = {
    Class.forName(driverClassName)
    val connection = DriverManager.getConnection(dbUrl, connectionProperties)
    val inserts = generateInserts(invalidRecords, invalidRecordsTableName)
    connection.createStatement().execute(inserts.mkString(";"))
    connection.close()
  }

}
