package com.datawizards.dqm.rules.group

import com.datawizards.dqm.configuration.ValidationContext
import com.datawizards.dqm.result.{GroupByStatistics, InvalidGroup}
import com.datawizards.dqm.rules.BusinessRule

trait GroupRule extends BusinessRule {
  def validate(groupByStatisticsList: Seq[GroupByStatistics], context: ValidationContext, groupName: String): Seq[InvalidGroup]
}
