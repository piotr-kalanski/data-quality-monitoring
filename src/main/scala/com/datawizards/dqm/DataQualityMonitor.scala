package com.datawizards.dqm

import com.datawizards.dqm.alert.AlertSender
import com.datawizards.dqm.configuration.loader.ConfigurationLoader
import com.datawizards.dqm.logger.ValidationResultLogger
import com.datawizards.dqm.validator.DataValidator

object DataQualityMonitor {
  def run(configurationLoader: ConfigurationLoader, validationResultLogger: ValidationResultLogger, alertSender: AlertSender): Unit = {
    val rules = configurationLoader.loadConfiguration()
    rules
      .tablesConfiguration
      .foreach{tc =>
        val result = DataValidator.validate(tc.location, tc.rules)
        validationResultLogger.log(result)
        alertSender.send(result)
      }
  }
}
