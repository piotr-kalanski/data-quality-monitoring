package com.datawizards.dqm.repository

import com.datawizards.dqm.alert.dto.SlackMessage
import org.json4s.{DefaultFormats, Formats}

import scalaj.http._
import org.json4s.jackson.Serialization

class SlackWebHookRepositoryImpl(webHookUrl: String) extends SlackWebHookRepository {
  implicit val formats: Formats = DefaultFormats

  override def sendMessage(message: SlackMessage): Unit = {
    val request = Http(webHookUrl).postData(Serialization.write(message))
    val response: HttpResponse[String] = request.asString
    if(response.code != 200) {
      throw new Exception(response.body)
    }
  }

}
