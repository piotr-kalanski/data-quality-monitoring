package com.datawizards.dqm.configuration.loader

import com.datawizards.dqm.configuration.location.HiveTableLocation
import com.datawizards.dqm.configuration.{DataQualityMonitoringConfiguration, TableConfiguration}
import com.datawizards.dqm.filter.FilterByYearMonthDayColumns
import com.datawizards.dqm.rules._
import com.datawizards.dqm.rules.field.{MaxRule, MinRule, NotNullRule}
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{FunSuite, Matchers}

@RunWith(classOf[JUnitRunner])
class DirectoryConfigurationLoaderTest extends FunSuite with Matchers {

  test("Load configuration from directory") {

    val configurationLoader = new DirectoryConfigurationLoader(getClass.getResource("/config_dir").getPath)
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

}
