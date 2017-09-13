package com.datawizards.dqm.configuration.install

import com.datawizards.dqm.configuration.{GroupByConfiguration, TableConfiguration}
import com.datawizards.dqm.configuration.location.{HiveTableLocation, TableLocation}
import com.datawizards.dqm.filter.{FilterByProcessingDateStrategy, FilterByYearMonthDayColumns}
import com.datawizards.dqm.rules._

trait ConfigurationInstaller {

  def installTableConfiguration(tableConfiguration: TableConfiguration): Unit = {
    installTableConfiguration(tableConfiguration.location.tableName, serializeTableConfiguration(tableConfiguration))
  }

  def installTablesConfiguration(tablesConfiguration: Seq[TableConfiguration]): Unit = {
    for(table <- tablesConfiguration)
      installTableConfiguration(table)
  }

  protected def installTableConfiguration(tableName: String, tableConfiguration: String): Unit

  private[install] def serializeTableConfiguration(tableConfiguration: TableConfiguration): String = {
    s"""{
      |${serializeTableLocation(tableConfiguration.location)}
      |${serializeFilterByProcessingDateStrategy(tableConfiguration.filterByProcessingDateStrategy)}
      |${serializeRules(tableConfiguration.rules)}
      |${serializeGroups(tableConfiguration.groups)}
      |}""".stripMargin
  }

  private def serializeTableLocation(location: TableLocation): String = {
    val locationSerialized = location match {
      case h:HiveTableLocation => s"type = Hive, table = ${h.table}"
      case _ => throw new RuntimeException("Not supported: " + location)
    }

    s"   location = {$locationSerialized}"
  }

  private def serializeFilterByProcessingDateStrategy(filterByProcessingDateStrategy: Option[FilterByProcessingDateStrategy]): String = {
    if(filterByProcessingDateStrategy.isEmpty) ""
    else {
      val filterSerialized =
        if(FilterByYearMonthDayColumns == FilterByYearMonthDayColumns) "type = ymd"
        else throw new RuntimeException("Not supported type: " + FilterByYearMonthDayColumns)

      s"   , filter = {$filterSerialized}"
    }
  }

  private def serializeRules(rules: TableRules): String = {
    if(rules.rowRules.isEmpty) ""
    else
      s"""   , rules = {
         |       rowRules = [
         |          ${rules.rowRules.map(rule => serializeFieldRules(rule)).mkString(",\n         ")}
         |       ]
         |     }""".stripMargin
  }

  private def serializeFieldRules(fieldRules: FieldRules): String = {
    s"""          {
       |             field = ${fieldRules.field},
       |             rules = [
       |                ${fieldRules.rules.map(rule => serializeFieldRule(rule)).mkString(",\n               ")}
       |             ]
       |          }""".stripMargin
  }

  private def serializeFieldRule(rule: FieldRule): String = {
    val ruleSerialized =
      if(rule == NotNullRule) "type = NotNull"
      else {
        rule match {
          case r:MinRule => s"type = min, value = ${r.min}"
          case r:MaxRule => s"type = max, value = ${r.max}"
          case r:DictionaryRule => s"type = dict, values = [${r.values.mkString(",")}]"
          case r:RegexRule => s"type = regex, value = ${r.regex}"
          case _ => throw new RuntimeException("Not supported type: " + rule)
        }
      }

    s"{$ruleSerialized}"
  }

  private def serializeGroups(groups: Seq[GroupByConfiguration]): String = {
    if(groups.isEmpty) ""
    else
      s"""   , groups = [
       |          ${groups.map(g => serializeGroup(g)).mkString("\n,         ")}
       |       ]""".stripMargin
  }

  private def serializeGroup(g: GroupByConfiguration): String = {
    s"""{
       |  name = ${g.groupName},
       |  field = ${g.groupByFieldName}
       |  ${if(g.rules.isEmpty) "" else s""", rules = [${g.rules.map(r => serializeGroupRule(r)).mkString(",\n         ")}]"""}
       |}""".stripMargin
  }

  private def serializeGroupRule(rule: GroupRule): String = {
    val serializedGroupRule = rule match {
      case neg:NotEmptyGroups => s"type = NotEmptyGroups, expectedGroups = [${neg.expectedGroups.mkString(",\n         ")}]"
      case _ => throw new RuntimeException("Not supported type: " + rule)
    }

    s"""{$serializedGroupRule}"""
  }

}
