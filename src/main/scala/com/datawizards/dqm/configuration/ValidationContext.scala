package com.datawizards.dqm.configuration

import java.sql.Date

case class ValidationContext(tableName: String, processingDate: Date) {
  private val localDate = processingDate.toLocalDate
  val processingYear: Int = localDate.getYear
  val processingMonth: Int = localDate.getMonthValue
  val processingDay: Int = localDate.getDayOfMonth
}
