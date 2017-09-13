package com.datawizards.dqm.filter

import java.sql.Date
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

case object FilterByYearMonthDayColumns extends FilterByProcessingDateStrategy {

  override def filter(input: DataFrame, processingDate: Date): DataFrame = {
    val localDate = processingDate.toLocalDate
    val year = localDate.getYear
    val month = localDate.getMonthValue
    val day = localDate.getDayOfMonth
    input.filter( (col("year") === lit(year)) && (col("month") === lit(month)) && (col("day") === lit(day)))
  }

}
