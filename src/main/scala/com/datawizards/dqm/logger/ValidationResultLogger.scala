package com.datawizards.dqm.logger

import com.datawizards.dqm.result._

trait ValidationResultLogger {

  def log(result: ValidationResult): Unit = {
    if(result.invalidRecords.nonEmpty)
      logInvalidRecords(result.invalidRecords)
    logTableStatistics(result.tableStatistics)
    logColumnStatistics(result.columnsStatistics)
    if(result.groupByStatisticsList.nonEmpty)
      logGroupByStatistics(result.groupByStatisticsList)
    if(result.invalidGroups.nonEmpty)
      logInvalidGroups(result.invalidGroups)
    if(result.invalidTableTrends.nonEmpty)
      logInvalidTableTrends(result.invalidTableTrends)
  }

  protected def logInvalidRecords(invalidRecords: Seq[InvalidRecord]): Unit
  protected def logTableStatistics(tableStatistics: TableStatistics): Unit
  protected def logColumnStatistics(columnsStatistics: Seq[ColumnStatistics]): Unit
  protected def logGroupByStatistics(groupByStatisticsList: Seq[GroupByStatistics]): Unit
  protected def logInvalidGroups(invalidGroups: Seq[InvalidGroup]): Unit
  protected def logInvalidTableTrends(invalidTableTrends: Seq[InvalidTableTrend]): Unit
}
