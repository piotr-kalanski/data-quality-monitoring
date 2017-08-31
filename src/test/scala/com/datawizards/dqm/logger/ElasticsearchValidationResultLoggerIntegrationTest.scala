package com.datawizards.dqm.logger

import com.datawizards.dqm.result.{InvalidRecord, ValidationResult}
import com.datawizards.esclient.repository.ElasticsearchRepositoryImpl
import org.scalatest.{FunSuite, Matchers}

class ElasticsearchValidationResultLoggerIntegrationTest extends FunSuite with Matchers {

  private val esUrl = "http://localhost:9200"
  private val repository = new ElasticsearchRepositoryImpl(esUrl)
  private val indexName = "invalid_records"
  private val logger = new ElasticsearchValidationResultLogger(esUrl, indexName: String)

  test("Elasticsearch logger integration tests") {
    // Run integration tests if Elasticsearch cluster is running
    if(repository.status()) {
      invalidRecordsTest()
    }
    else {
      println("Elasticsearch instance not running!")
    }
  }

  private def invalidRecordsTest(): Unit = {
    repository.deleteIndexIfNotExists(indexName)
    val invalidRecords = Seq(
      InvalidRecord("{c:value}", "value", "NOT NULL")
    )
    logger.log(ValidationResult(
      invalidRecords = invalidRecords
    ))
    Thread.sleep(1000L)
    val results = repository.search[InvalidRecord](indexName)
    results.total should equal(invalidRecords.size)
    results.hits should equal(invalidRecords)
  }

}
