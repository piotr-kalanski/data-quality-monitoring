package com.datawizards.dqm.alert.dto

case class SlackMessage(
                         text: String,
                         channel: String,
                         username: String,
                         attachments: Seq[SlackMessageAttachment] = Seq.empty
                       )
