package com.datawizards.dqm.configuration.loader

import com.datawizards.dqm.configuration.DataQualityMonitoringConfiguration

class StaticConfigurationLoader(rules: DataQualityMonitoringConfiguration) extends ConfigurationLoader {
  override def loadConfiguration(): DataQualityMonitoringConfiguration = rules
}
