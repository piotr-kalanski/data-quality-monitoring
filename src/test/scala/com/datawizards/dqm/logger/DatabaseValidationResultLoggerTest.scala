package com.datawizards.dqm.logger

import java.sql.DriverManager
import java.util.Properties

import com.datawizards.dqm.result.{InvalidRecord, ValidationResult}
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
        |)
      """.stripMargin)
    val logger = new DatabaseValidationResultLogger(
      driverClassName = "org.h2.Driver",
      dbUrl = connectionString,
      connectionProperties = new Properties(),
      invalidRecordsTableName = "INVALID_RECORDS"
    )
    val invalidRecords = Seq(
      InvalidRecord("{c:value}", "value", "NOT NULL")
    )
    logger.log(ValidationResult(
      invalidRecords = invalidRecords,
      null,
      null
      // TODO - add additional fields
    ))
    val result = selectTable[InvalidRecord](connection, "INVALID_RECORDS")._1
    connection.close()

    result should equal(invalidRecords)
  }

}
