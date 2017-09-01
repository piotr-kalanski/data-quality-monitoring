package com.datawizards.dqm.logger

import com.datawizards.dqm.result.{InvalidRecord, ValidationResult}

trait ValidationResultLogger {

  def log(result: ValidationResult): Unit = {
    logInvalidRecords(result.invalidRecords)
  }

  protected def logInvalidRecords(invalidRecords: Seq[InvalidRecord]): Unit

  // TODO - add logging table and column statistics
}
