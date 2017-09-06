package com.datawizards.dqm.configuration.loader

import java.sql.DriverManager
import java.util.Properties

import com.datawizards.dqm.configuration.DataQualityMonitoringConfiguration
import com.typesafe.config.{Config, ConfigFactory, ConfigList}

import scala.collection.mutable.ListBuffer

/**
  * Loads configuration from database table.
  * <br/>
  * One table row should contain configuration for one table (TableConfiguration).
  * <br/>
  * Expected DB schema for configurationTableName:
  * <pre>
  * CREATE TABLE VALIDATION_RULES(
  *   tableConfiguration VARCHAR
  * )
  * </pre>
  *
  * @param driverClassName JDBC driver class name
  * @param dbUrl DB connection string
  * @param connectionProperties JDBC connection properties, especially user and password
  * @param configurationTableName name of table with configuration
  */
class DatabaseConfigurationLoader(
                             driverClassName: String,
                             dbUrl: String,
                             connectionProperties: Properties,
                             configurationTableName: String
                           ) extends ConfigurationLoader {

  private val CONFIGURATION_COLUMN: String = "tableConfiguration"

  override def loadConfiguration(): DataQualityMonitoringConfiguration = {
    val configuration = readConfiguration()
    DataQualityMonitoringConfiguration(parseTablesConfiguration(configuration))
  }

  protected def readConfiguration(): Seq[Config] = {
    Class.forName(driverClassName)
    val connection = DriverManager.getConnection(dbUrl, connectionProperties)
    val rs = connection.createStatement().executeQuery("SELECT * FROM " + configurationTableName)
    val result = new ListBuffer[Config]
    while(rs.next()) {
      result += ConfigFactory.parseString(rs.getString(CONFIGURATION_COLUMN))
    }
    connection.close()
    result.toList
  }

}
