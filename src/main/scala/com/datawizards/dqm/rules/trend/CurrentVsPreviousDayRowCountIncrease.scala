package com.datawizards.dqm.rules.trend

import com.datawizards.dqm.configuration.ValidationContext
import com.datawizards.dqm.result.{InvalidTableTrend, TableStatistics}

case class CurrentVsPreviousDayRowCountIncrease(percentageThreshold: Int) extends TableTrendRule {
  override def name = "CurrentVsPreviousDayRowCountIncrease"

  override def validate(tableStatisticsList: Seq[TableStatistics], context: ValidationContext): Seq[InvalidTableTrend] = {
    val previousDay = context.processingDate.toLocalDate.minusDays(1)
    val statisticsForCurrentTable = tableStatisticsList.filter(s => s.tableName == context.tableName)

    val currentDayStatitics = statisticsForCurrentTable
      .find(s =>
        s.year == context.processingYear && s.month == context.processingMonth && s.day == context.processingDay
      )

    val previousDayStatitics = statisticsForCurrentTable
      .find(s =>
        s.year == previousDay.getYear && s.month == previousDay.getMonthValue && s.day == previousDay.getDayOfMonth
      )

    if(currentDayStatitics.isEmpty || previousDayStatitics.isEmpty) Seq.empty
    else {
      val previousRowCount = previousDayStatitics.get.rowsCount
      val currentRowCount = currentDayStatitics.get.rowsCount
      val proportion = 1.0 * currentRowCount / previousRowCount
      if((proportion >= 1.0 + percentageThreshold/100.0) || (proportion <= 1.0 - percentageThreshold/100.0))
        Seq(
          InvalidTableTrend(
            tableName = context.tableName,
            rule = name,
            comment = s"$previousRowCount -> $currentRowCount rows",
            year = context.processingYear,
            month = context.processingMonth,
            day = context.processingDay
          )
        )
      else
        Seq.empty
    }
  }

}
