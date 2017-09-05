package com.datawizards.dqm.dto

case class SlackMessageAttachment(
                                   color: String,
                                   title: String,
                                   text: String,
                                   fallback: String,
                                   footer: String,
                                   ts: Long
                                 )
