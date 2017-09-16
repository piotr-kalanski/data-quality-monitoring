package com.datawizards.dqm.rules.trend

import com.datawizards.dqm.configuration.ValidationContext
import com.datawizards.dqm.result.{InvalidTableTrend, TableStatistics}
import com.datawizards.dqm.rules.BusinessRule

trait TableTrendRule extends BusinessRule {
  def validate(tableStatisticsList: Seq[TableStatistics], context: ValidationContext): Seq[InvalidTableTrend]
}
