package com.datawizards.dqm.alert

import com.datawizards.dqm.result._

trait AlertSender {

  def send(result: ValidationResult): Unit = {
    if(result.tableStatistics.rowsCount == 0)
      sendAlertEmptyTable(result.tableStatistics)
    val emptyColumns = result.columnsStatistics.filter(_.notMissingCount == 0)
    if(emptyColumns.nonEmpty)
      sendAlertEmptyColumns(emptyColumns)
    if(result.invalidRecords.nonEmpty)
      sendAlertInvalidRecords(result.invalidRecords)
    if(result.invalidGroups.nonEmpty)
      sendAlertInvalidGroups(result.invalidGroups)
    if(result.invalidTableTrends.nonEmpty)
      sendAlertInvalidTableTrends(result.invalidTableTrends)
  }

  protected def sendAlertEmptyTable(tableStatistics: TableStatistics): Unit
  protected def sendAlertEmptyColumns(emptyColumns: Seq[ColumnStatistics]): Unit
  protected def sendAlertInvalidRecords(invalidRecords: Seq[InvalidRecord]): Unit
  protected def sendAlertInvalidGroups(invalidGroups: Seq[InvalidGroup]): Unit
  protected def sendAlertInvalidTableTrends(invalidTableTrends: Seq[InvalidTableTrend]): Unit

}
