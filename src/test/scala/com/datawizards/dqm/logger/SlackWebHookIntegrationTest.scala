package com.datawizards.dqm.logger

import java.time.{LocalDateTime, ZoneId}
import com.datawizards.dqm.dto.{SlackMessage, SlackMessageAttachment}
import com.datawizards.dqm.repository.SlackWebHookRepositoryImpl

object SlackWebHookIntegrationTest extends App {
  val repository = new SlackWebHookRepositoryImpl("https://hooks.slack.com/services/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")
  repository.sendMessage(SlackMessage(
    text = "test",
    channel = "alerts",
    username = "test bot",
    attachments = Seq(
      SlackMessageAttachment(
        color = "#FF0000",
        title = "test title",
        text = "test text",
        fallback = "test fallback",
        footer = "test footer",
        ts = LocalDateTime.parse(LocalDateTime.now.toString).atZone(ZoneId.systemDefault).toEpochSecond
      )
    )
  ))
}
