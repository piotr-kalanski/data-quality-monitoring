package com.datawizards.dqm.logger

import com.datawizards.dqm.result.ValidationResult

trait ValidationResultLogger {
  def log(result: ValidationResult): Unit
}
