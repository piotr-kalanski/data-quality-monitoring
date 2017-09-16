package com.datawizards.dqm.alert

import java.time.{LocalDateTime, ZoneId}

import com.datawizards.dqm.alert.dto.{SlackMessage, SlackMessageAttachment}
import com.datawizards.dqm.repository.SlackWebHookRepositoryImpl
import com.datawizards.dqm.result._

class SlackAlertSender(webHookUrl: String, slackChannel: String, slackUserName: String) extends AlertSender {

  private val slackRepository = new SlackWebHookRepositoryImpl(webHookUrl)

  override protected def sendAlertEmptyTable(tableStatistics: TableStatistics): Unit = {
    slackRepository.sendMessage(SlackMessage(
      text = s"Empty table [${tableStatistics.tableName}] for date ${tableStatistics.year}-${tableStatistics.month}-${tableStatistics.day}",
      channel = slackChannel,
      username = slackUserName
    ))
  }

  override protected def sendAlertEmptyColumns(emptyColumns: Seq[ColumnStatistics]): Unit = {
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

  override protected def sendAlertInvalidRecords(invalidRecords: Seq[InvalidRecord]): Unit = {
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

  override protected def sendAlertInvalidGroups(invalidGroups: Seq[InvalidGroup]): Unit = {
    slackRepository.sendMessage(SlackMessage(
      text = s"Invalid groups for table [${invalidGroups.head.tableName}] for date ${invalidGroups.head.year}-${invalidGroups.head.month}-${invalidGroups.head.day}",
      channel = slackChannel,
      username = slackUserName,
      attachments = invalidGroups.map {r =>
        SlackMessageAttachment(
          color = "#FF0000",
          title = r.rule,
          text = s"${r.groupName} = ${r.groupValue.getOrElse("")}",
          fallback = null,
          footer = null,
          ts = LocalDateTime.parse(LocalDateTime.now.toString).atZone(ZoneId.systemDefault).toEpochSecond
        )
      }
    ))
  }

  override protected def sendAlertInvalidTableTrends(invalidTableTrends: Seq[InvalidTableTrend]): Unit = {
    slackRepository.sendMessage(SlackMessage(
      text = s"Invalid trends for table [${invalidTableTrends.head.tableName}] for date ${invalidTableTrends.head.year}-${invalidTableTrends.head.month}-${invalidTableTrends.head.day}",
      channel = slackChannel,
      username = slackUserName,
      attachments = invalidTableTrends.map {r =>
        SlackMessageAttachment(
          color = "#FF0000",
          title = r.rule,
          text = r.comment,
          fallback = null,
          footer = null,
          ts = LocalDateTime.parse(LocalDateTime.now.toString).atZone(ZoneId.systemDefault).toEpochSecond
        )
      }
    ))
  }

}
