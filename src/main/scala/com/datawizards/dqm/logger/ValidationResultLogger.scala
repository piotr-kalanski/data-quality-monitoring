package com.datawizards.dqm.logger

import com.datawizards.dqm.result._

trait ValidationResultLogger {

  def log(result: ValidationResult): Unit = {
    logInvalidRecords(result.invalidRecords)
    logTableStatistics(result.tableStatistics)
    logColumnStatistics(result.columnsStatistics)
    logGroupByStatistics(result.groupByStatisticsList)
    logInvalidGroups(result.invalidGroups)
  }

  protected def logInvalidRecords(invalidRecords: Seq[InvalidRecord]): Unit
  protected def logTableStatistics(tableStatistics: TableStatistics): Unit
  protected def logColumnStatistics(columnsStatistics: Seq[ColumnStatistics]): Unit
  protected def logGroupByStatistics(groupByStatisticsList: Seq[GroupByStatistics]): Unit
  protected def logInvalidGroups(invalidGroups: Seq[InvalidGroup]): Unit
}
