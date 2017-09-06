package com.datawizards.dqm.configuration

import java.sql.DriverManager
import java.util.Properties

import com.datawizards.dqm.configuration.loader.DatabaseConfigurationLoader
import com.datawizards.dqm.configuration.location.HiveTableLocation
import com.datawizards.dqm.rules._
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{FunSuite, Matchers}

@RunWith(classOf[JUnitRunner])
class DatabaseConfigurationLoaderTest extends FunSuite with Matchers {

  test("Load configuration from DB - complex") {
    Class.forName("org.h2.Driver")
    val connectionString = "jdbc:h2:mem:test"
    val connection = DriverManager.getConnection(connectionString, "", "")
    connection.createStatement().execute(
      """CREATE TABLE VALIDATION_RULES(
        |   tableConfiguration VARCHAR
        |);
        |
        |INSERT INTO VALIDATION_RULES VALUES('{
        |    location = {type = Hive, table = clients},
        |    rules = {
        |      rowRules = [
        |        {
        |          field = client_id,
        |          rules = [
        |            {type = NotNull},
        |            {type = min, value = 0}
        |          ]
        |        },
        |        {
        |          field = client_name,
        |          rules = [
        |            {type = NotNull}
        |          ]
        |        }
        |      ]
        |    }
        |  }');
        |INSERT INTO VALIDATION_RULES VALUES('{
        |    location = {type = Hive, table = companies},
        |    rules = {
        |      rowRules = [
        |        {
        |          field = company_id,
        |          rules = [
        |            {type = NotNull},
        |            {type = max, value = 100}
        |          ]
        |        },
        |        {
        |          field = company_name,
        |          rules = [
        |            {type = NotNull}
        |          ]
        |        }
        |      ]
        |    }
        |  }');
      """.stripMargin)

    val configurationLoader = new DatabaseConfigurationLoader(
      driverClassName = "org.h2.Driver",
      dbUrl = connectionString,
      connectionProperties = new Properties(),
      configurationTableName = "VALIDATION_RULES"
    )
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
    connection.close()
  }

}
