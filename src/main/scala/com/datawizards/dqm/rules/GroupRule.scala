package com.datawizards.dqm.rules

import com.datawizards.dqm.configuration.ValidationContext
import com.datawizards.dqm.result.{GroupByStatistics, InvalidGroup}

trait GroupRule extends BusinessRule {
  def name: String
  def validate(groupByStatisticsList: Seq[GroupByStatistics], context: ValidationContext): Seq[InvalidGroup]
}
