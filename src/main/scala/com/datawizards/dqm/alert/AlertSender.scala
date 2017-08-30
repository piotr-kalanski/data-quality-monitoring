package com.datawizards.dqm.alert

import com.datawizards.dqm.result.ValidationResult

trait AlertSender {
  def send(result: ValidationResult): Unit
}
