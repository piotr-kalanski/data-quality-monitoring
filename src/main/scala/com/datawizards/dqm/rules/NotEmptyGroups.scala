package com.datawizards.dqm.rules
import com.datawizards.dqm.result.GroupByStatistics

case class NotEmptyGroups(expectedGroups: Seq[String]) extends GroupRule {

  override def name: String = "NotEmptyGroups"

  override def validate(groupByStatisticsList: Seq[GroupByStatistics]): Boolean = {
    expectedGroups.forall(group => groupByStatisticsList.exists(_.groupByFieldValue == group))
  }

}
