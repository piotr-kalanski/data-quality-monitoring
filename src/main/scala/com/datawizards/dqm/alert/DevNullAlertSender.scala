package com.datawizards.dqm.alert

import com.datawizards.dqm.result._

object DevNullAlertSender extends AlertSender {

  override protected def sendAlertEmptyTable(tableStatistics: TableStatistics): Unit = { /* do nothing */ }

  override protected def sendAlertEmptyColumns(emptyColumns: Seq[ColumnStatistics]): Unit = { /* do nothing */ }

  override protected def sendAlertInvalidRecords(invalidRecords: Seq[InvalidRecord]): Unit = { /* do nothing */ }

  override protected def sendAlertInvalidGroups(invalidGroups: Seq[InvalidGroup]): Unit = { /* do nothing */ }
}
