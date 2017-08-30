package com.datawizards.dqm.configuration

class StaticConfigurationLoader(rules: DataQualityMonitoringConfiguration) extends ConfigurationLoader {
  override def loadConfiguration(): DataQualityMonitoringConfiguration = rules
}
