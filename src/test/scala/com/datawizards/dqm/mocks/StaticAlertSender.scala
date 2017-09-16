package com.datawizards.dqm.mocks

import com.datawizards.dqm.alert.AlertSender
import com.datawizards.dqm.result._

import scala.collection.mutable.ListBuffer

class StaticAlertSender extends AlertSender {
  val results = new ListBuffer[ValidationResult]

  override def send(result: ValidationResult): Unit =
    results += result

  override protected def sendAlertEmptyTable(tableStatistics: TableStatistics): Unit = { /* do nothing */ }

  override protected def sendAlertEmptyColumns(emptyColumns: Seq[ColumnStatistics]): Unit = { /* do nothing */ }

  override protected def sendAlertInvalidRecords(invalidRecords: Seq[InvalidRecord]): Unit = { /* do nothing */ }

  override protected def sendAlertInvalidGroups(invalidGroups: Seq[InvalidGroup]): Unit = { /* do nothing */ }

  override protected def sendAlertInvalidTableTrends(invalidTableTrends: Seq[InvalidTableTrend]): Unit = { /* do nothing */ }
}
