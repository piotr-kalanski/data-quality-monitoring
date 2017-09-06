package com.datawizards.dqm.logger

import java.sql.Date

import com.datawizards.dqm.result.{ColumnStatistics, InvalidRecord, TableStatistics, ValidationResult}
import com.datawizards.esclient.repository.ElasticsearchRepositoryImpl
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{FunSuite, Matchers}

@RunWith(classOf[JUnitRunner])
class ElasticsearchValidationResultLoggerIntegrationTest extends FunSuite with Matchers {

  private val esUrl = "http://localhost:9200"
  private val repository = new ElasticsearchRepositoryImpl(esUrl)
  private val invalidRecordsIndexName = "invalid_records"
  private val tableStatisticsIndexName = "table_statistics"
  private val columnStatisticsIndexName = "column_statistics"
  private val logger = new ElasticsearchValidationResultLogger(esUrl, invalidRecordsIndexName, tableStatisticsIndexName, columnStatisticsIndexName)

  test("Elasticsearch logger integration tests") {
    // Run integration tests if Elasticsearch cluster is running
    if(repository.status()) {
      runTest()
    }
    else {
      println("Elasticsearch instance not running!")
    }
  }

  private def runTest(): Unit = {
    repository.deleteIndexIfNotExists(invalidRecordsIndexName)
    repository.deleteIndexIfNotExists(tableStatisticsIndexName)
    repository.deleteIndexIfNotExists(columnStatisticsIndexName)
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
    Thread.sleep(1000L)

    val resultInvalidRecords = repository.search[InvalidRecord](invalidRecordsIndexName)
    resultInvalidRecords.hits should equal(invalidRecords)

    val resultTableStatistics = repository.search[TableStatistics](tableStatisticsIndexName)
    resultTableStatistics.hits should equal(Seq(tableStatistics))

    val resultColumnStatistics = repository.search[ColumnStatistics](columnStatisticsIndexName)
    resultColumnStatistics.hits.toSeq.sortBy(_.columnName) should equal(columnsStatistics)
  }

}
