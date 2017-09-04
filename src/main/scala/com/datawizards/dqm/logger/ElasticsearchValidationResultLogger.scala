package com.datawizards.dqm.logger

import com.datawizards.dqm.configuration.location.ColumnStatistics
import com.datawizards.dqm.result.{InvalidRecord, TableStatistics}
import com.datawizards.esclient.repository.ElasticsearchRepositoryImpl

/**
  * Validation results logger saving result in Elasticsearch
  *
  * @param esUrl Elasticsearch URL
  * @param invalidRecordsIndexName Index name where to store invalid records
  * @param tableStatisticsIndexName Index name where to store table statistics
  * @param columnStatisticsIndexName Index name where to store column statistics
  */
class ElasticsearchValidationResultLogger(esUrl: String, invalidRecordsIndexName: String, tableStatisticsIndexName: String, columnStatisticsIndexName: String) extends ValidationResultLogger {
  private lazy val esRepository = new ElasticsearchRepositoryImpl(esUrl)

  override protected def logInvalidRecords(invalidRecords: Seq[InvalidRecord]): Unit = {
    for(record <- invalidRecords)
      esRepository.index(
        indexName = invalidRecordsIndexName,
        typeName = "invalid_record",
        documentId = java.util.UUID.randomUUID().toString,
        document = record
      )
  }

  override protected def logTableStatistics(tableStatistics: TableStatistics): Unit = {
      esRepository.index(
        indexName = tableStatisticsIndexName,
        typeName = "table",
        documentId = java.util.UUID.randomUUID().toString,
        document = tableStatistics)
  }

  override protected def logColumnStatistics(columnsStatistics: Seq[ColumnStatistics]): Unit = {
    for(column <- columnsStatistics)
      esRepository.index(
        indexName = columnStatisticsIndexName,
        typeName = "column",
        documentId = java.util.UUID.randomUUID().toString,
        document = column
      )
  }
}
