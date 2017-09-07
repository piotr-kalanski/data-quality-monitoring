package com.datawizards.dqm.rules

import com.datawizards.dqm.result.GroupByStatistics

trait GroupRule extends BusinessRule {
  def name: String
  def validate(groupByStatisticsList: Seq[GroupByStatistics]): Boolean
}
