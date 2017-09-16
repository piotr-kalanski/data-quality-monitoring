package com.datawizards.dqm.rules.trend

import com.datawizards.dqm.configuration.ValidationContext
import com.datawizards.dqm.result.{InvalidTableTrend, TableStatistics}
import org.junit.runner.RunWith
import org.scalatest.{FunSuite, Matchers}
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class CurrentVsPreviousDayRowCountIncreaseTest extends FunSuite with Matchers {

    test("Validate table trend - current vs previous day row count increase") {
      val rule10 = CurrentVsPreviousDayRowCountIncrease(10)
      val rule20 = CurrentVsPreviousDayRowCountIncrease(20)
      val rule30 = CurrentVsPreviousDayRowCountIncrease(30)
      val tableStatisticsList = Seq(
        TableStatistics(
          tableName = "table",
          rowsCount = 100,
          columnsCount = 10,
          year = 2016,
          month = 1,
          day = 1
        ),
        TableStatistics(
          tableName = "table",
          rowsCount = 115,
          columnsCount = 10,
          year = 2016,
          month = 1,
          day = 2
        ),
        TableStatistics(
          tableName = "table",
          rowsCount = 140,
          columnsCount = 10,
          year = 2016,
          month = 1,
          day = 3
        )
      )

      val processingDateDay2 = java.sql.Date.valueOf("2016-01-02")
      val processingDateDay3 = java.sql.Date.valueOf("2016-01-03")
      val contextDay2 = ValidationContext("table", processingDateDay2)
      val contextDay3 = ValidationContext("table", processingDateDay3)

      rule10.validate(tableStatisticsList, contextDay2) should equal(Seq(
        InvalidTableTrend("table", rule10.name, "100 -> 115 rows", 2016, 1, 2)
      ))
      rule10.validate(tableStatisticsList, contextDay3) should equal(Seq(
        InvalidTableTrend("table", rule10.name, "115 -> 140 rows", 2016, 1, 3)
      ))

      rule20.validate(tableStatisticsList, contextDay2) should equal(Seq.empty)
      rule20.validate(tableStatisticsList, contextDay3) should equal(Seq(
        InvalidTableTrend("table", rule20.name, "115 -> 140 rows", 2016, 1, 3)
      ))

      rule30.validate(tableStatisticsList, contextDay2) should equal(Seq.empty)
      rule30.validate(tableStatisticsList, contextDay3) should equal(Seq.empty)
    }

  test("Validate table trend - current vs previous day row count decrease") {
    val rule10 = CurrentVsPreviousDayRowCountIncrease(10)
    val rule20 = CurrentVsPreviousDayRowCountIncrease(20)
    val rule30 = CurrentVsPreviousDayRowCountIncrease(30)
    val tableStatisticsList = Seq(
      TableStatistics(
        tableName = "table",
        rowsCount = 100,
        columnsCount = 10,
        year = 2016,
        month = 1,
        day = 1
      ),
      TableStatistics(
        tableName = "table",
        rowsCount = 85,
        columnsCount = 10,
        year = 2016,
        month = 1,
        day = 2
      ),
      TableStatistics(
        tableName = "table",
        rowsCount = 65,
        columnsCount = 10,
        year = 2016,
        month = 1,
        day = 3
      )
    )

    val processingDateDay2 = java.sql.Date.valueOf("2016-01-02")
    val processingDateDay3 = java.sql.Date.valueOf("2016-01-03")
    val contextDay2 = ValidationContext("table", processingDateDay2)
    val contextDay3 = ValidationContext("table", processingDateDay3)

    rule10.validate(tableStatisticsList, contextDay2) should equal(Seq(
      InvalidTableTrend("table", rule10.name, "100 -> 85 rows", 2016, 1, 2)
    ))
    rule10.validate(tableStatisticsList, contextDay3) should equal(Seq(
      InvalidTableTrend("table", rule10.name, "85 -> 65 rows", 2016, 1, 3)
    ))

    rule20.validate(tableStatisticsList, contextDay2) should equal(Seq.empty)
    rule20.validate(tableStatisticsList, contextDay3) should equal(Seq(
      InvalidTableTrend("table", rule20.name, "85 -> 65 rows", 2016, 1, 3)
    ))

    rule30.validate(tableStatisticsList, contextDay2) should equal(Seq.empty)
    rule30.validate(tableStatisticsList, contextDay3) should equal(Seq.empty)
  }

}
