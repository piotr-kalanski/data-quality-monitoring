package com.datawizards.dqm.logger

import com.datawizards.dqm.result.ValidationResult

import scala.collection.mutable.ListBuffer

class StaticValidationResultLogger extends ValidationResultLogger {
  val results = new ListBuffer[ValidationResult]

  override def log(result: ValidationResult): Unit =
    results += result

}
