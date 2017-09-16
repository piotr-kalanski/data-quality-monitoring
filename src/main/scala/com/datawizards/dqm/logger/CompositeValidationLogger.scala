package com.datawizards.dqm.logger

import com.datawizards.dqm.result._

class CompositeValidationLogger(loggers: Seq[ValidationResultLogger]) extends ValidationResultLogger {

  override def log(result: ValidationResult): Unit =
    loggers.foreach(_.log(result))

  override protected def logInvalidRecords(invalidRecords: Seq[InvalidRecord]): Unit = { /* do nothing */ }

  override protected def logTableStatistics(tableStatistics: TableStatistics): Unit = { /* do nothing */ }

  override protected def logColumnStatistics(columnsStatistics: Seq[ColumnStatistics]): Unit = { /* do nothing */ }

  override protected def logGroupByStatistics(groupByStatisticsList: Seq[GroupByStatistics]): Unit = { /* do nothing */ }

  override protected def logInvalidGroups(invalidGroups: Seq[InvalidGroup]): Unit = { /* do nothing */ }

  override protected def logInvalidTableTrends(invalidTableTrends: Seq[InvalidTableTrend]): Unit = { /* do nothing */ }
}
