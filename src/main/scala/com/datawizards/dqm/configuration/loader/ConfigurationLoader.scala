package com.datawizards.dqm.configuration.loader

import com.datawizards.dqm.configuration.location.{HiveTableLocation, TableLocation}
import com.datawizards.dqm.configuration.{DataQualityMonitoringConfiguration, TableConfiguration}
import com.datawizards.dqm.rules._
import com.typesafe.config.{Config, ConfigList, ConfigValue}
import scala.collection.JavaConversions._

trait ConfigurationLoader {
  def loadConfiguration(): DataQualityMonitoringConfiguration

  protected def parseTablesConfiguration(tablesConfiguration: ConfigList): Seq[TableConfiguration] = {
    for(tableConfiguration <- tablesConfiguration)
      yield parseTableConfiguration(tableConfiguration)
  }

  protected def parseTablesConfiguration(tablesConfiguration: Seq[Config]): Seq[TableConfiguration] = {
    for(tableConfiguration <- tablesConfiguration)
      yield parseTableConfiguration(tableConfiguration)
  }

  protected def parseTableConfiguration(tableConfigValue: ConfigValue): TableConfiguration = {
    val tableConfiguration = tableConfigValue.atKey("table")
    TableConfiguration(
      location = parseLocation(tableConfiguration.getConfig("table.location")),
      rules = parseTableRules(tableConfiguration.getConfig("table.rules"))
    )
  }

  protected def parseTableConfiguration(tableConfiguration: Config): TableConfiguration = {
    TableConfiguration(
      location = parseLocation(tableConfiguration.getConfig("location")),
      rules = parseTableRules(tableConfiguration.getConfig("rules"))
    )
  }

  protected def parseTableRules(tableRules: Config): TableRules = {
    val rowRulesConfigList = tableRules.getConfigList("rowRules")
    val rowRules = for(fieldRules <- rowRulesConfigList)
      yield parseFieldRules(fieldRules)
    TableRules(rowRules = rowRules)
  }

  protected def parseFieldRules(fieldRules: Config): FieldRules = {
    val field = fieldRules.getString("field")
    val rules = for(rule <- fieldRules.getConfigList("rules"))
      yield parseRule(rule)
    FieldRules(
      field = field,
      rules = rules
    )
  }

  protected def parseLocation(cfg: Config): TableLocation = {
    val tableLocationType = cfg.getString("type")
    if(tableLocationType == "Hive") HiveTableLocation(cfg.getString("table"))
    else throw new RuntimeException("Not supported type: " + tableLocationType)
  }

  protected def parseRule(cfg: Config): FieldRule = {
    val ruleType = cfg.getString("type")
    if(ruleType == "NotNull") NotNullRule
    else if(ruleType == "min") MinRule(cfg.getString("value"))
    else if(ruleType == "max") MaxRule(cfg.getString("value"))
    else if(ruleType == "dict") DictionaryRule(cfg.getStringList("values"))
    else if(ruleType == "regex") RegexRule(cfg.getString("value"))
    else throw new RuntimeException("Not supported type: " + ruleType)
  }

}
