package com.datawizards.dqm.logger

import com.datawizards.dqm.configuration.location.ColumnStatistics
import com.datawizards.dqm.result.{InvalidRecord, TableStatistics, ValidationResult}

import scala.collection.mutable.ListBuffer

class StaticValidationResultLogger extends ValidationResultLogger {
  val results = new ListBuffer[ValidationResult]

  override def log(result: ValidationResult): Unit =
    results += result

  override protected def logInvalidRecords(invalidRecords: Seq[InvalidRecord]): Unit = { /* do nothing */ }

  override protected def logTableStatistics(tableStatistics: TableStatistics): Unit = { /* do nothing */ }

  override protected def logColumnStatistics(columnsStatistics: Seq[ColumnStatistics]): Unit = { /* do nothing */ }
}
