package com.datawizards.dqm.configuration.install

import java.sql.DriverManager
import java.util.Properties

/**
  * Insert configuration to database table.
  * <pre>
  * CREATE TABLE VALIDATION_RULES(
  *   tableName VARCHAR,
  *   tableConfiguration VARCHAR
  * )
  * </pre>
  *
  * @param driverClassName JDBC driver class name
  * @param dbUrl DB connection string
  * @param connectionProperties JDBC connection properties, especially user and password
  * @param configurationTableName name of table with configuration
  */
class DatabaseConfigurationInstaller(
                                      driverClassName: String,
                                      dbUrl: String,
                                      connectionProperties: Properties,
                                      configurationTableName: String
                                    ) extends ConfigurationInstaller {

  override def installTableConfiguration(tableName: String, tableConfiguration: String): Unit = {
    Class.forName(driverClassName)
    val connection = DriverManager.getConnection(dbUrl, connectionProperties)

    val deleteStatement = s"DELETE FROM $configurationTableName WHERE tableName = '$tableName'; "
    val insertStatement = s"INSERT INTO $configurationTableName VALUES('$tableName', '$tableConfiguration');"

    connection.createStatement().execute(deleteStatement + insertStatement)
    connection.close()
  }

}
