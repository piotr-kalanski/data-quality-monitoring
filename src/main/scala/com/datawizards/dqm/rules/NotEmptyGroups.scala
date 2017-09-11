package com.datawizards.dqm.rules
import com.datawizards.dqm.configuration.ValidationContext
import com.datawizards.dqm.result.{GroupByStatistics, InvalidGroup}

case class NotEmptyGroups(expectedGroups: Seq[String]) extends GroupRule {

  override def name: String = "NotEmptyGroups"

  override def validate(groupByStatisticsList: Seq[GroupByStatistics], context: ValidationContext): Seq[InvalidGroup] = {
    expectedGroups
        .withFilter(g => groupByStatisticsList.forall(_.groupByFieldValue != g))
        .map{g =>
          InvalidGroup(
            tableName = context.tableName,
            groupName = groupByStatisticsList.head.groupName,
            groupValue = Some(g),
            rule = name,
            year = context.processingYear,
            month = context.processingMonth,
            day = context.processingDay
          )
        }
  }

}
