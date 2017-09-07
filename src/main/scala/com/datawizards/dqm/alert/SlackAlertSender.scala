package com.datawizards.dqm.alert

import java.time.{LocalDateTime, ZoneId}
import com.datawizards.dqm.dto.{SlackMessage, SlackMessageAttachment}
import com.datawizards.dqm.repository.SlackWebHookRepositoryImpl
import com.datawizards.dqm.result.{ColumnStatistics, InvalidRecord, TableStatistics, ValidationResult}

class SlackAlertSender(webHookUrl: String, slackChannel: String, slackUserName: String) extends AlertSender {

  private val slackRepository = new SlackWebHookRepositoryImpl(webHookUrl)

  override def send(result: ValidationResult): Unit = {
    if(result.tableStatistics.rowsCount == 0)
      sendAlertEmptyTable(result.tableStatistics)
    val emptyColumns = result.columnsStatistics.filter(_.notMissingCount == 0)
    if(emptyColumns.nonEmpty)
      sendAlertEmptyColumns(emptyColumns)
    if(result.invalidRecords.nonEmpty)
      sendAlertInvalidRecords(result.invalidRecords)
  }

  private def sendAlertEmptyTable(tableStatistics: TableStatistics): Unit = {
    slackRepository.sendMessage(SlackMessage(
      text = s"Empty table [${tableStatistics.tableName}] for date ${tableStatistics.year}-${tableStatistics.month}-${tableStatistics.day}",
      channel = slackChannel,
      username = slackUserName
    ))
  }

  private def sendAlertEmptyColumns(emptyColumns: Seq[ColumnStatistics]): Unit = {
    slackRepository.sendMessage(SlackMessage(
      text = s"Empty columns for table [${emptyColumns.head.tableName}] for date ${emptyColumns.head.year}-${emptyColumns.head.month}-${emptyColumns.head.day}",
      channel = slackChannel,
      username = slackUserName,
      attachments = emptyColumns.map {c =>
        SlackMessageAttachment(
          color = "#FF0000",
          title = c.columnName,
          text = null,
          fallback = null,
          footer = null,
          ts = LocalDateTime.parse(LocalDateTime.now.toString).atZone(ZoneId.systemDefault).toEpochSecond
        )
      }
    ))
  }

  private def sendAlertInvalidRecords(invalidRecords: Seq[InvalidRecord]): Unit = {
    slackRepository.sendMessage(SlackMessage(
      text = s"Invalid records for table [${invalidRecords.head.tableName}] for date ${invalidRecords.head.year}-${invalidRecords.head.month}-${invalidRecords.head.day}",
      channel = slackChannel,
      username = slackUserName,
      attachments = invalidRecords.take(10).map {r =>
        SlackMessageAttachment(
          color = "#FF0000",
          title = r.rule,
          text = s"${r.columnName} = ${r.value}",
          fallback = null,
          footer = null,
          ts = LocalDateTime.parse(LocalDateTime.now.toString).atZone(ZoneId.systemDefault).toEpochSecond
        )
      }
    ))
  }

}
