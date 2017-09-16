package com.datawizards.dqm.configuration.loader

import com.datawizards.dqm.configuration.location.HiveTableLocation
import com.datawizards.dqm.configuration.{DataQualityMonitoringConfiguration, TableConfiguration}
import com.datawizards.dqm.rules._
import com.datawizards.dqm.rules.field.NotNullRule
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{FunSuite, Matchers}

@RunWith(classOf[JUnitRunner])
class FileSingleTableConfigurationLoaderTest extends FunSuite with Matchers {

  test("Load configuration from file - simple") {
    val configurationLoader = new FileSingleTableConfigurationLoader(getClass.getResource("/configuration_single_table.conf").getPath)
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
}
