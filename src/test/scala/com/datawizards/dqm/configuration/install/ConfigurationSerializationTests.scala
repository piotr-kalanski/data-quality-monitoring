package com.datawizards.dqm.configuration.install

import com.datawizards.dqm.configuration.{GroupByConfiguration, TableConfiguration}
import com.datawizards.dqm.configuration.location.HiveTableLocation
import com.datawizards.dqm.filter.FilterByYearMonthDayColumns
import com.datawizards.dqm.rules._
import com.datawizards.dqm.rules.field.{MinRule, NotNullRule}
import com.datawizards.dqm.rules.group.NotEmptyGroups
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{FunSuite, Matchers}

@RunWith(classOf[JUnitRunner])
class ConfigurationSerializationTests extends FunSuite with Matchers {

  private val installer = new ConfigurationInstaller {
    override protected def installTableConfiguration(tableName: String, tableConfiguration: String): Unit = {}
  }

  test("Serialize configuration - simple") {
    val configuration = TableConfiguration(
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
    val expectedResult =
      """{
        |  location = {type = Hive, table = clients},
        |  rules = {
        |    rowRules = [
        |      {
        |        field = client_id,
        |        rules = [
        |          {type = NotNull}
        |        ]
        |      }
        |    ]
        |  }
        |}
      """.stripMargin.replace(" ", "").replace("\n", "").replace("\r", "")

    serialize(configuration) should equal(expectedResult)
  }

  test("Serialize configuration - complex") {
    val configuration = TableConfiguration(
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
    )
    val expectedResult =
      """{
        |  location = {type = Hive, table = clients},
        |  filter = {type = ymd},
        |  rules = {
        |    rowRules = [
        |      {
        |        field = client_id,
        |        rules = [
        |          {type = NotNull},
        |          {type = min, value = 0}
        |        ]
        |      },
        |      {
        |        field = client_name,
        |        rules = [
        |          {type = NotNull}
        |        ]
        |      }
        |    ]
        |  }
        |}
      """.stripMargin.replace(" ", "").replace("\n", "").replace("\r", "")

    serialize(configuration) should equal(expectedResult)
  }

  test("Serialize configuration - group rules") {
    val configuration = TableConfiguration(
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
    val expectedResult =
      """{
        |  location = {type = Hive, table = clients},
        |  rules = {
        |    rowRules = [
        |      {
        |        field = client_id,
        |        rules = [
        |          {type = NotNull}
        |        ]
        |      }
        |    ]
        |  },
        |  groups = [
        |    {
        |      name = COUNTRY,
        |      field = country,
        |      rules = [
        |        {
        |          type = NotEmptyGroups,
        |          expectedGroups = [c1,c2,c3,c4]
        |        }
        |      ]
        |    },
        |    {
        |      name = GENDER,
        |      field = gender
        |    }
        |  ]
        |}
      """.stripMargin.replace(" ", "").replace("\n", "").replace("\r", "")

    serialize(configuration) should equal(expectedResult)
  }

  private def serialize(tableConfiguration: TableConfiguration): String =
    installer.serializeTableConfiguration(tableConfiguration).replace(" ", "").replace("\n", "").replace("\r", "")

}
