package com.datawizards.dqm.configuration

import com.datawizards.dqm.configuration.loader.FileConfigurationLoader
import com.datawizards.dqm.configuration.location.HiveTableLocation
import com.datawizards.dqm.rules._
import org.scalatest.{FunSuite, Matchers}

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

  test("Load configuration from file - dictionary") {
    val configurationLoader = new FileConfigurationLoader(getClass.getResource("/configuration_simple_dict.conf").getPath)
    val expectedConfiguration = DataQualityMonitoringConfiguration(
      tablesConfiguration = Seq(
        TableConfiguration(
          location = HiveTableLocation("clients"),
          rules = TableRules(
            rowRules = Seq(
              FieldRules(
                field = "dict",
                rules = Seq(
                  DictionaryRule(Seq("1", "2", "3"))
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

}
