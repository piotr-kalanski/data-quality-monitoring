package com.datawizards.dqm

import java.sql.Date

import com.datawizards.dqm.alert.AlertSender
import com.datawizards.dqm.configuration.loader.ConfigurationLoader
import com.datawizards.dqm.logger.ValidationResultLogger
import com.datawizards.dqm.validator.DataValidator

object DataQualityMonitor {
  def run(processingDate: Date, configurationLoader: ConfigurationLoader, validationResultLogger: ValidationResultLogger, alertSender: AlertSender): Unit = {
    val rules = configurationLoader.loadConfiguration()
    rules
      .tablesConfiguration
      .foreach{tc =>
        val result = DataValidator.validate(tc, processingDate)
        validationResultLogger.log(result)
        alertSender.send(result)
      }
  }
}
