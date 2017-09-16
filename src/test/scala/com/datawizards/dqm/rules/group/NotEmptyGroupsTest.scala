package com.datawizards.dqm.rules.group

import java.sql.Date

import com.datawizards.dqm.configuration.ValidationContext
import com.datawizards.dqm.result.{GroupByStatistics, InvalidGroup}
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{FunSuite, Matchers}

@RunWith(classOf[JUnitRunner])
class NotEmptyGroupsTest extends FunSuite with Matchers {
  test("Validate not empty groups") {
    val rule = NotEmptyGroups(Seq("c1", "c2", "c3", "c4"))
    val statistics = Seq(
      GroupByStatistics(
        tableName = "t1",
        groupName = "country",
        groupByFieldValue = "c1",
        rowsCount = 1,
        year = 2000,
        month = 1,
        day = 2
      ),
      GroupByStatistics(
        tableName = "t1",
        groupName = "country",
        groupByFieldValue = "c2",
        rowsCount = 1,
        year = 2000,
        month = 1,
        day = 2
      )
    )
    val expectedInvalidGroups = Seq(
      InvalidGroup(
        tableName = "t1",
        groupName = "country",
        groupValue = Some("c3"),
        rule = "NotEmptyGroups",
        year = 2000,
        month = 1,
        day = 2
      ),
      InvalidGroup(
        tableName = "t1",
        groupName = "country",
        groupValue = Some("c4"),
        rule = "NotEmptyGroups",
        year = 2000,
        month = 1,
        day = 2
      )
    )
    val processingDate = Date.valueOf("2000-01-02")
    rule.validate(statistics, ValidationContext("t1", processingDate)) should equal(expectedInvalidGroups)
  }
}
