package com.datawizards.dqm.logger

import com.datawizards.dqm.configuration.location.ColumnStatistics
import com.datawizards.dqm.result.{InvalidRecord, TableStatistics, ValidationResult}

trait ValidationResultLogger {

  def log(result: ValidationResult): Unit = {
    logInvalidRecords(result.invalidRecords)
    logTableStatistics(result.tableStatistics)
    logColumnStatistics(result.columnsStatistics)
  }

  protected def logInvalidRecords(invalidRecords: Seq[InvalidRecord]): Unit

  protected def logTableStatistics(tableStatistics: TableStatistics): Unit

  protected def logColumnStatistics(columnsStatistics: Seq[ColumnStatistics]): Unit
}
