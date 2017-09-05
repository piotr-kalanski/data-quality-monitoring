package com.datawizards.dqm.dto

case class SlackMessage(
                         text: String,
                         channel: String,
                         username: String,
                         attachments: Seq[SlackMessageAttachment]
                       )
