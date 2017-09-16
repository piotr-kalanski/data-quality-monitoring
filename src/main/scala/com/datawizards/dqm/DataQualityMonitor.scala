package com.datawizards.dqm

import java.sql.Date

import com.datawizards.dqm.alert.AlertSender
import com.datawizards.dqm.configuration.ValidationContext
import com.datawizards.dqm.configuration.loader.ConfigurationLoader
import com.datawizards.dqm.history.HistoryStatisticsReader
import com.datawizards.dqm.logger.ValidationResultLogger
import com.datawizards.dqm.validator.DataValidator
import org.apache.log4j.Logger

object DataQualityMonitor {
  private val log = Logger.getLogger(getClass.getName)

  /**
    * Run data quality monitoring
    */
  def run(
           processingDate: Date,
           configurationLoader: ConfigurationLoader,
           historyStatisticsReader: HistoryStatisticsReader,
           validationResultLogger: ValidationResultLogger,
           alertSender: AlertSender
         ): Unit = {
    log.info("Starting Data Quality Monitor")
    log.info("Loading configuration")
    val rules = configurationLoader.loadConfiguration()
    log.info("Configuration: " + rules.tablesConfiguration.mkString(", "))
    rules
      .tablesConfiguration
      .foreach{tc =>
        log.info("Validating table: " + tc.location.tableName)
        val context = ValidationContext(tc.location.tableName, processingDate)
        val result = DataValidator.validate(tc, context, historyStatisticsReader)
        log.info("Logging validation results for table: " + tc.location.tableName)
        validationResultLogger.log(result)
        log.info("Sending alerts for table: " + tc.location.tableName)
        alertSender.send(result)
      }
  }

}
