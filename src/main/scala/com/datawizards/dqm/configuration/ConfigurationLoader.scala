package com.datawizards.dqm.configuration

trait ConfigurationLoader {
  def loadConfiguration(): DataQualityMonitoringConfiguration
}
