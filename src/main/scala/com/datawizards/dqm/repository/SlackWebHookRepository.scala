package com.datawizards.dqm.repository

import com.datawizards.dqm.dto.SlackMessage

trait SlackWebHookRepository {
  def sendMessage(message: SlackMessage): Unit
}
