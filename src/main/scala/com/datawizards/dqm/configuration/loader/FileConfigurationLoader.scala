package com.datawizards.dqm.configuration.loader

import java.io.File

import com.datawizards.dqm.configuration.DataQualityMonitoringConfiguration
import com.typesafe.config.{Config, ConfigFactory}

/**
  * Loads configuration from file.
  * <br/>
  * Expected format:
  * <pre>
  *tablesConfiguration = [
    {
      location = {type = Hive, table = clients},
      rules = {
        rowRules = [
          {
            field = client_id,
            rules = [
              {type = NotNull},
              {type = min, value = 0}
            ]
          },
          {
            field = client_name,
            rules = [
              {type = NotNull}
            ]
          }
        ]
      }
    },
    {
      location = {type = Hive, table = companies},
      rules = {
        rowRules = [
          {
            field = company_id,
            rules = [
              {type = NotNull},
              {type = max, value = 100}
            ]
          },
          {
            field = company_name,
            rules = [
              {type = NotNull}
            ]
          }
        ]
      }
    }
  ]
  * <pre>
  *
  * @param path configuration file
  */
class FileConfigurationLoader(path: String) extends ConfigurationLoader {

  override def loadConfiguration(): DataQualityMonitoringConfiguration = {
    val config = ConfigFactory.parseFile(new File(path))
    parseConfig(config)
  }

  private def parseConfig(config: Config): DataQualityMonitoringConfiguration = {
    val tablesConfiguration = config.getList("tablesConfiguration")
    DataQualityMonitoringConfiguration(parseTablesConfiguration(tablesConfiguration))
  }

}
