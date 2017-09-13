package com.datawizards.dqm.configuration.install

import java.sql.DriverManager
import java.util.Properties

import com.datawizards.dqm.configuration.TableConfiguration
import com.datawizards.dqm.configuration.loader.DatabaseConfigurationLoader
import com.datawizards.dqm.configuration.location.HiveTableLocation
import com.datawizards.dqm.rules._
import com.datawizards.jdbc2class._
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{FunSuite, Matchers}

@RunWith(classOf[JUnitRunner])
class DatabaseConfigurationInstallerTest extends FunSuite with Matchers {

  test("Load configuration from DB - complex") {
    Class.forName("org.h2.Driver")
    val connectionString = "jdbc:h2:mem:test"
    val connection = DriverManager.getConnection(connectionString, "", "")
    connection.createStatement().execute(
      """CREATE TABLE VALIDATION_RULES_INSTALLER(
        |   tableName VARCHAR,
        |   tableConfiguration VARCHAR
        |);
      """.stripMargin)

    val configurationInstaller = new DatabaseConfigurationInstaller(
      driverClassName = "org.h2.Driver",
      dbUrl = connectionString,
      connectionProperties = new Properties(),
      configurationTableName = "VALIDATION_RULES_INSTALLER"
    )
    val configurationLoader = new DatabaseConfigurationLoader(
      driverClassName = "org.h2.Driver",
      dbUrl = connectionString,
      connectionProperties = new Properties(),
      configurationTableName = "VALIDATION_RULES_INSTALLER"
    )

    val tablesConfiguration = Seq(
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

    // Run first time
    configurationInstaller.installTablesConfiguration(tablesConfiguration)
    configurationLoader.loadConfiguration().tablesConfiguration should equal(tablesConfiguration)

    val newTablesConfiguration = Seq(
      TableConfiguration(
        location = HiveTableLocation("clients"),
        rules = TableRules(
          rowRules = Seq(
            FieldRules(
              field = "client_id2",
              rules = Seq(
                NotNullRule
              )
            ),
            FieldRules(
              field = "client_name2",
              rules = Seq(
                NotNullRule,
                MinRule("0")
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
              field = "company_id2",
              rules = Seq(
                NotNullRule
              )
            ),
            FieldRules(
              field = "company_name2",
              rules = Seq(
                NotNullRule,
                MaxRule("100")
              )
            )
          )
        )
      )
    )

    // Running second time should replace previous rules
    configurationInstaller.installTablesConfiguration(newTablesConfiguration)
    configurationLoader.loadConfiguration().tablesConfiguration should equal(newTablesConfiguration)

    connection.close()
  }

}
