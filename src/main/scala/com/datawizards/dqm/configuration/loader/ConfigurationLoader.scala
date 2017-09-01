package com.datawizards.dqm.configuration.loader

import com.datawizards.dqm.configuration.DataQualityMonitoringConfiguration

trait ConfigurationLoader {
  def loadConfiguration(): DataQualityMonitoringConfiguration
}
