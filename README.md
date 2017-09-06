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
    - [Load configuration from file](#load-configuration-from-file)
    - [Load configuration from database](#load-configuration-from-database)
  - [Validation rules](#validation-rules)
  - [Log validation results](#log-validation-results)
  - [Send alerts](#send-alerts)

# Goals

- Validate data using provided business rules
- Log result
- Send alerts

# Getting started

Include dependency:

```scala
"com.github.piotr-kalanski" % "data-quality-monitoring_2.11" % "0.1.0"
```

or

```xml
<dependency>
    <groupId>com.github.piotr-kalanski</groupId>
    <artifactId>data-quality-monitoring_2.11</artifactId>
    <version>0.1.0</version>
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
- RDBMS

Additionally there are plans to support:
- directory
- Dynamo DB

### Load configuration from file

Use class: ```FileConfigurationLoader```

Example configuration:
```
tablesConfiguration = [
  {
    location = {type = Hive, table = clients},
    rules = {
      rowRules = [
        {
          field = client_id,
          rules = [
            {type = NotNull},
            {type = min, value = 0}
          ]
        },
        {
          field = client_name,
          rules = [
            {type = NotNull}
          ]
        }
      ]
    }
  },
  {
    location = {type = Hive, table = companies},
    rules = {
      rowRules = [
        {
          field = company_id,
          rules = [
            {type = NotNull},
            {type = max, value = 100}
          ]
        },
        {
          field = company_name,
          rules = [
            {type = NotNull}
          ]
        }
      ]
    }
  }
]
```

### Load configuration from database

Use class: `DatabaseConfigurationLoader`.

One table row should contain configuration for one table (TableConfiguration).

## Validation rules

Supported validation rules:
- not null
- dictionary
- regex
- min value
- max value

## Log validation results

Validation results can be logged into:
- Elasticsearch using class `ElasticsearchValidationResultLogger`
- RDBMS using class `DatabaseValidationResultLogger`

## Send alerts

Alerts can be send to:
- Slack using class `SlackAlertSender`
- email

