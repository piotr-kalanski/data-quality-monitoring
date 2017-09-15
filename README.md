
# data-quality-monitoring
Data Quality Monitoring Tool for Big Data implemented using Spark

[![Build Status](https://api.travis-ci.org/piotr-kalanski/data-quality-monitoring.png?branch=development)](https://api.travis-ci.org/piotr-kalanski/data-quality-monitoring.png?branch=development)
[![codecov.io](http://codecov.io/github/piotr-kalanski/data-quality-monitoring/coverage.svg?branch=development)](http://codecov.io/github/piotr-kalanski/data-quality-monitoring/coverage.svg?branch=development)
[<img src="https://img.shields.io/maven-central/v/com.github.piotr-kalanski/data-quality-monitoring_2.11.svg?label=latest%20release"/>](http://search.maven.org/#search%7Cga%7C1%7Ca%3A%22data-quality-monitoring_2.11%22)
[![License](http://img.shields.io/:license-Apache%202-red.svg)](http://www.apache.org/licenses/LICENSE-2.0.txt)

# Table of contents

- [Goals](#goals)
- [Getting started](#getting-started)
- [Data quality monitoring process](#data-quality-monitoring-process)
  - [Load configuration](#load-configuration)
    - [Example configuration](#example-configuration)
    - [Load configuration from file](#load-configuration-from-file)
    - [Load configuration from directory](#load-configuration-from-directory)
    - [Load configuration from database](#load-configuration-from-database)
  - [Validation rules](#validation-rules)
  - [Log validation results](#log-validation-results)
  - [Send alerts](#send-alerts)
- [Full example](#full-example)

# Goals

- Validate data using provided business rules
- Log result
- Send alerts

# Getting started

Include dependency:

```scala
"com.github.piotr-kalanski" % "data-quality-monitoring_2.11" % "0.2.3"
```

or

```xml
<dependency>
    <groupId>com.github.piotr-kalanski</groupId>
    <artifactId>data-quality-monitoring_2.11</artifactId>
    <version>0.2.3</version>
</dependency>
```

# Data quality monitoring process

Data quality monitoring process consists from below steps:
- Load configuration with business rules
- Run data validation
- Log validation results
- Send alerts

## Load configuration

Configuration can be loaded from:
- file
- directory
- RDBMS

Additionally there are plans to support:
- Dynamo DB

### Example configuration

```
tablesConfiguration = [
  {
    location = {type = Hive, table = clients}, // location of first table that should be validated
    rules = { // validation rules 
      rowRules = [ // validation rules working on single row level
        {
          field = client_id, // name of field that should be validated
          rules = [
            {type = NotNull}, // this field shouldn't be null
            {type = min, value = 0} // minimum value for this field is 0
          ]
        },
        {
          field = client_name,
          rules = [
            {type = NotNull} // this field shouldn't be null
          ]
        }
      ]
    }
  },
  {
    location = {type = Hive, table = companies}, // location of first table that should be validated
    rules = {
      rowRules = [
        {
          field = company_id, // name of field that should be validated
          rules = [
            {type = NotNull}, // this field shouldn't be null
            {type = max, value = 100} // maximum value for this field is 100
          ]
        },
        {
          field = company_name, // name of field that should be validated
          rules = [
            {type = NotNull} // this field shouldn't be null
          ]
        }
      ]
    }
  }
]
```

### Load configuration from file

Use class: ```FileSingleTableConfigurationLoader``` or ```FileMultipleTablesConfigurationLoader```.

Example:
```scala
import com.datawizards.dqm.configuration.loader.FileMultipleTablesConfigurationLoader
val configurationLoader = new FileMultipleTablesConfigurationLoader("configuration.conf")
configurationLoader.loadConfiguration()
```

### Load configuration from directory

Use class: `DirectoryConfigurationLoader`.

One file should contain configuration for one table (TableConfiguration).

### Load configuration from database

Use class: `DatabaseConfigurationLoader`.

One table row should contain configuration for one table (TableConfiguration).

## Validation rules

Supported validation rules:
- not null

    ```{type = NotNull}```

- dictionary

    ```{type = dict, values=[1,2,3]}```
    
- regex

    ```{type = regex, value = """\s.*"""}```
    
- min value

    ```{type = min, value = 0}```
    
- max value

    ```{type = max, value = 100}```

## Log validation results

Validation results can be logged into:
- Elasticsearch using class `ElasticsearchValidationResultLogger`

    ```scala  
    val logger = new ElasticsearchValidationResultLogger(
        esUrl = "http://localhost:9200", // Elasticsearch URL
        invalidRecordsIndexName = "invalid_records", // Index name where to store invalid records
        tableStatisticsIndexName = "table_statistics", // Index name where to store table statistics
        columnStatisticsIndexName = "column_statistics", // Index name where to store column statistics
        groupsStatisticsIndexName = "group_statistics", // Index name where to store group statistics
        invalidGroupsIndexName = "invalid_groups" // Index name where to store group statistics
    )
    ```
    
- RDBMS using class `DatabaseValidationResultLogger`

    ```scala
  
    val logger = new DatabaseValidationResultLogger(
      driverClassName = "org.h2.Driver", // JDBC driver class name
      dbUrl = connectionString, // DB connection string
      connectionProperties = new Properties(), // JDBC connection properties, especially user and password
      invalidRecordsTableName = "INVALID_RECORDS", // name of table where to insert invalid records
      tableStatisticsTableName = "TABLE_STATISTICS", // name of table where to insert table statistics records
      columnStatisticsTableName = "COLUMN_STATISTICS", // name of table where to insert column statistics records
      groupsStatisticsTableName = "GROUP_STATISTICS", // name of table where to insert group by statistics records
      invalidGroupsTableName = "INVALID_GROUPS" // name of table where to insert invalid groups
    )
    ```

## Send alerts

Alerts can be send to:
- Slack using class `SlackAlertSender`

Additionally there are plans to support:
- email

# Full example

## Example

```scala
import com.datawizards.dqm.configuration.loader.FileConfigurationLoader
import com.datawizards.dqm.logger.ElasticsearchValidationResultLogger
import com.datawizards.dqm.alert.SlackAlertSender
import com.datawizards.dqm.DataQualityMonitor

val configurationLoader = new FileConfigurationLoader("configuration.conf")
val esUrl = "http://localhost:9200"
val invalidRecordsIndexName = "invalid_records"
val tableStatisticsIndexName = "table_statistics"
val columnStatisticsIndexName = "column_statistics"
val groupsStatisticsIndexName = "group_statistics"
val invalidGroupsIndexName = "invalid_groups"
private val logger = new ElasticsearchValidationResultLogger(esUrl, invalidRecordsIndexName, tableStatisticsIndexName, columnStatisticsIndexName, groupsStatisticsIndexName, invalidGroupsIndexName)
val alertSender = new SlackAlertSender("webhook url", "Slack channel", "Slack user name")
val processingDate = new java.util.Date()
DataQualityMonitor.run(processingDate, configurationLoader, logger, alertSender)
```

configuration.conf:
```
tablesConfiguration = [
  {
    location = {type = Hive, table = clients},
    rules = {
      rowRules = [
        {
          field = client_id,
          rules = [
            {type = NotNull}
          ]
        }
      ]
    }
  }
]
```