package com.datawizards.dqm.alert

import com.datawizards.dqm.result.ValidationResult

object DevNullAlertSender extends AlertSender {
  def send(result: ValidationResult): Unit = { /* do nothing */ }
}
