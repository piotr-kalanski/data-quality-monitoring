package com.datawizards.dqm.logger

import com.datawizards.dqm.result._

trait ValidationResultLogger {

  def log(result: ValidationResult): Unit = {
    logInvalidRecords(result.invalidRecords)
    logTableStatistics(result.tableStatistics)
    logColumnStatistics(result.columnsStatistics)
    logGroupByStatistics(result.groupByStatisticsList)
  }

  protected def logInvalidRecords(invalidRecords: Seq[InvalidRecord]): Unit
  protected def logTableStatistics(tableStatistics: TableStatistics): Unit
  protected def logColumnStatistics(columnsStatistics: Seq[ColumnStatistics]): Unit
  protected def logGroupByStatistics(groupByStatisticsList: Seq[GroupByStatistics]): Unit

}
