package com.datawizards.dqm.configuration.loader

import com.datawizards.dqm.configuration.location.{HiveTableLocation, TableLocation}
import com.datawizards.dqm.configuration.{DataQualityMonitoringConfiguration, GroupByConfiguration, TableConfiguration}
import com.datawizards.dqm.filter.{FilterByProcessingDateStrategy, FilterByYearMonthDayColumns}
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
    parseTableConfiguration(tableConfiguration, "table.location", "table.rules", "table.filter", "table.groups")
  }

  protected def parseTableConfiguration(tableConfiguration: Config): TableConfiguration = {
    parseTableConfiguration(tableConfiguration, "location", "rules", "filter", "groups")
  }

  private def parseTableConfiguration(tableConfiguration: Config, locationPath: String, rulesPath: String, filterPath: String, groupsPath: String): TableConfiguration = {
    TableConfiguration(
      location = parseLocation(tableConfiguration.getConfig(locationPath)),
      rules = parseTableRules(tableConfiguration.getConfig(rulesPath)),
      filterByProcessingDateStrategy = parseFilterByProcessingDateStrategy(tableConfiguration, filterPath),
      groups = parseGroups(tableConfiguration, groupsPath)
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
    else if(ruleType == "max") MaxRule(cfg.getString("value"))
    else if(ruleType == "dict") DictionaryRule(cfg.getStringList("values"))
    else if(ruleType == "regex") RegexRule(cfg.getString("value"))
    else throw new RuntimeException("Not supported type: " + ruleType)
  }

  private def parseFilterByProcessingDateStrategy(tableConfiguration: Config, filterPath: String): Option[FilterByProcessingDateStrategy] = {
    if(!tableConfiguration.hasPath(filterPath)) None
    else {
      val filterConfig = tableConfiguration.getConfig(filterPath)
      val filterType = filterConfig.getString("type")
      if(filterType == "ymd") Some(FilterByYearMonthDayColumns)
      else throw new RuntimeException("Not supported type: " + filterType)
    }
  }

  private def parseGroups(tableConfiguration: Config, groupsPath: String): Seq[GroupByConfiguration] = {
    if(!tableConfiguration.hasPath(groupsPath))
      Seq.empty
    else {
      val groupConfigs = tableConfiguration.getConfigList(groupsPath).toList
      groupConfigs.map{cfg =>
        GroupByConfiguration(
          groupName = cfg.getString("name"),
          groupByFieldName = cfg.getString("field")
        )
      }
    }
  }

}
