package com.datawizards.dqm.configuration

import java.io.File

import com.typesafe.config.{Config, ConfigFactory, ConfigList, ConfigValue}
import com.datawizards.dqm.configuration.location._
import com.datawizards.dqm.rules._

import scala.collection.JavaConversions._

class FileConfigurationLoader(path: String) extends ConfigurationLoader {

  override def loadConfiguration(): DataQualityMonitoringConfiguration = {
    val config = ConfigFactory.parseFile(new File(path))
    parseConfig(config)
  }

  private def parseConfig(config: Config): DataQualityMonitoringConfiguration = {
    val tablesConfiguration = config.getList("tablesConfiguration")
    DataQualityMonitoringConfiguration(parseTablesConfiguration(tablesConfiguration))
  }

  private def parseTablesConfiguration(tablesConfiguration: ConfigList): Seq[TableConfiguration] = {
    for(tableConfiguration <- tablesConfiguration)
      yield parseTableConfiguration(tableConfiguration)
  }

  private def parseTableConfiguration(tableConfigValue: ConfigValue): TableConfiguration = {
    val tableConfiguration = tableConfigValue.atKey("table")
    TableConfiguration(
      location = parseLocation(tableConfiguration.getConfig("table.location")),
      rules = parseTableRules(tableConfiguration.getConfig("table.rules"))
    )
  }

  private def parseTableRules(tableRules: Config): TableRules = {
    val rowRulesConfigList = tableRules.getConfigList("rowRules")
    val rowRules = for(fieldRules <- rowRulesConfigList)
      yield parseFieldRules(fieldRules)
    TableRules(rowRules = rowRules)
  }

  private def parseFieldRules(fieldRules: Config): FieldRules = {
    val field = fieldRules.getString("field")
    val rules = for(rule <- fieldRules.getConfigList("rules"))
      yield parseRule(rule)
    FieldRules(
      field = field,
      rules = rules
    )
  }

  private def parseLocation(cfg: Config): TableLocation = {
    val tableLocationType = cfg.getString("type")
    if(tableLocationType == "Hive") HiveTableLocation(cfg.getString("table"))
    else throw new RuntimeException("Not supported type: " + tableLocationType)
  }

  private def parseRule(cfg: Config): FieldRule = {
    val ruleType = cfg.getString("type")
    if(ruleType == "NotNull") NotNullRule
    else if(ruleType == "min") MinRule(cfg.getString("value"))
    else throw new RuntimeException("Not supported type: " + ruleType)
  }
}
