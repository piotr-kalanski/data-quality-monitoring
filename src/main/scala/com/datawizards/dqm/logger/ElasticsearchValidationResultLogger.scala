package com.datawizards.dqm.logger

import com.datawizards.dqm.result._
import com.datawizards.esclient.repository.ElasticsearchRepositoryImpl

/**
  * Validation results logger saving result in Elasticsearch
  *
  * @param esUrl Elasticsearch URL
  * @param invalidRecordsIndexName Index name where to store invalid records
  * @param tableStatisticsIndexName Index name where to store table statistics
  * @param columnStatisticsIndexName Index name where to store column statistics
  * @param groupsStatisticsIndexName Index name where to store group statistics
  * @param invalidGroupsIndexName Index name where to store invalid groups records
  * @param invalidTableTrendsIndexName Index name where to store invalid table trends records
  */
class ElasticsearchValidationResultLogger(
                                           esUrl: String,
                                           invalidRecordsIndexName: String,
                                           tableStatisticsIndexName: String,
                                           columnStatisticsIndexName: String,
                                           groupsStatisticsIndexName: String,
                                           invalidGroupsIndexName: String,
                                           invalidTableTrendsIndexName: String
                                         ) extends ValidationResultLogger {
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

  override protected def logGroupByStatistics(groupByStatisticsList: Seq[GroupByStatistics]): Unit = {
    for(groupByStatistics <- groupByStatisticsList)
      esRepository.index(
        indexName = groupsStatisticsIndexName,
        typeName = "group_statistics",
        documentId = java.util.UUID.randomUUID().toString,
        document = groupByStatistics
      )
  }

  override protected def logInvalidGroups(invalidGroups: Seq[InvalidGroup]): Unit = {
    for(group <- invalidGroups)
      esRepository.index(
        indexName = invalidGroupsIndexName,
        typeName = "group",
        documentId = java.util.UUID.randomUUID().toString,
        document = group
      )
  }

  override protected def logInvalidTableTrends(invalidTableTrends: Seq[InvalidTableTrend]): Unit = {
    for(invalidTableTrend <- invalidTableTrends)
      esRepository.index(
        indexName = invalidTableTrendsIndexName,
        typeName = "invalid_trend",
        documentId = java.util.UUID.randomUUID().toString,
        document = invalidTableTrend
      )
  }

}
