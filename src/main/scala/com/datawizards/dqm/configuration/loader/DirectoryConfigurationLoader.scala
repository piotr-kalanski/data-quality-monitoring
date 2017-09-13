package com.datawizards.dqm.configuration.loader

import java.io.File
import com.datawizards.dqm.configuration.DataQualityMonitoringConfiguration
import com.typesafe.config.{Config, ConfigFactory}

/**
  * Loads configuration from provided path. Each file in directory corresponds to table configuration
  *
  * @param path directory with configuration files
  */
class DirectoryConfigurationLoader(path: String) extends ConfigurationLoader {

  override def loadConfiguration(): DataQualityMonitoringConfiguration = {
    val configuration = readConfiguration()
    DataQualityMonitoringConfiguration(parseTablesConfiguration(configuration))
  }

  protected def readConfiguration(): Seq[Config] = {
    val dir = new File(path)
    dir
      .listFiles()
      .map(f => ConfigFactory.parseFile(f))
  }

}
