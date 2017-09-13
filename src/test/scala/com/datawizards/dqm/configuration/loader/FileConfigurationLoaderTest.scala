package com.datawizards.dqm.configuration.loader

import com.datawizards.dqm.configuration.location.HiveTableLocation
import com.datawizards.dqm.configuration.{DataQualityMonitoringConfiguration, GroupByConfiguration, TableConfiguration}
import com.datawizards.dqm.filter.FilterByYearMonthDayColumns
import com.datawizards.dqm.rules._
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{FunSuite, Matchers}

@RunWith(classOf[JUnitRunner])
class FileConfigurationLoaderTest extends FunSuite with Matchers {

  test("Load configuration from file - simple") {
    val configurationLoader = new FileConfigurationLoader(getClass.getResource("/configuration_simple.conf").getPath)
    val expectedConfiguration = DataQualityMonitoringConfiguration(
      tablesConfiguration = Seq(
        TableConfiguration(
          location = HiveTableLocation("clients"),
          rules = TableRules(
            rowRules = Seq(
              FieldRules(
                field = "client_id",
                rules = Seq(
                  NotNullRule
                )
              )
            )
          )
        )
      )
    )
    configurationLoader.loadConfiguration() should equal(expectedConfiguration)
  }

  test("Load configuration from file - all rules") {
    val configurationLoader = new FileConfigurationLoader(getClass.getResource("/configuration_all_rules.conf").getPath)
    val expectedConfiguration = DataQualityMonitoringConfiguration(
      tablesConfiguration = Seq(
        TableConfiguration(
          location = HiveTableLocation("clients"),
          rules = TableRules(
            rowRules = Seq(
              FieldRules(
                field = "client_id",
                rules = Seq(
                  NotNullRule,
                  MinRule("0"),
                  MaxRule("100"),
                  DictionaryRule(Seq("1", "2", "3")),
                  RegexRule("""\s.*""")
                )
              )
            )
          )
        )
      )
    )
    configurationLoader.loadConfiguration() should equal(expectedConfiguration)
  }

  test("Load configuration from file - complex") {
    val configurationLoader = new FileConfigurationLoader(getClass.getResource("/configuration_complex.conf").getPath)
    val expectedConfiguration = DataQualityMonitoringConfiguration(
      tablesConfiguration = Seq(
        TableConfiguration(
          location = HiveTableLocation("clients"),
          filterByProcessingDateStrategy = Some(FilterByYearMonthDayColumns),
          rules = TableRules(
            rowRules = Seq(
              FieldRules(
                field = "client_id",
                rules = Seq(
                  NotNullRule,
                  MinRule("0")
                )
              ),
              FieldRules(
                field = "client_name",
                rules = Seq(
                  NotNullRule
                )
              )
            )
          )
        ),
        TableConfiguration(
          location = HiveTableLocation("companies"),
          rules = TableRules(
            rowRules = Seq(
              FieldRules(
                field = "company_id",
                rules = Seq(
                  NotNullRule,
                  MaxRule("100")
                )
              ),
              FieldRules(
                field = "company_name",
                rules = Seq(
                  NotNullRule
                )
              )
            )
          )
        )
      )
    )
    configurationLoader.loadConfiguration() should equal(expectedConfiguration)
  }

  test("Load configuration from file - groups") {
    val configurationLoader = new FileConfigurationLoader(getClass.getResource("/configuration_with_groups.conf").getPath)
    val expectedConfiguration = DataQualityMonitoringConfiguration(
      tablesConfiguration = Seq(
        TableConfiguration(
          location = HiveTableLocation("clients"),
          rules = TableRules(
            rowRules = Seq(
              FieldRules(
                field = "client_id",
                rules = Seq(
                  NotNullRule
                )
              )
            )
          ),
          groups = Seq(
            GroupByConfiguration("COUNTRY", "country"),
            GroupByConfiguration("GENDER", "gender")
          )
        )
      )
    )
    configurationLoader.loadConfiguration() should equal(expectedConfiguration)
  }

  test("Load configuration from file - groups with rules") {
    val configurationLoader = new FileConfigurationLoader(getClass.getResource("/configuration_with_groups_rules.conf").getPath)
    val expectedConfiguration = DataQualityMonitoringConfiguration(
      tablesConfiguration = Seq(
        TableConfiguration(
          location = HiveTableLocation("clients"),
          rules = TableRules(
            rowRules = Seq(
              FieldRules(
                field = "client_id",
                rules = Seq(
                  NotNullRule
                )
              )
            )
          ),
          groups = Seq(
            GroupByConfiguration("COUNTRY", "country", Seq(NotEmptyGroups(Seq("c1","c2","c3","c4")))),
            GroupByConfiguration("GENDER", "gender")
          )
        )
      )
    )
    configurationLoader.loadConfiguration() should equal(expectedConfiguration)
  }

}
