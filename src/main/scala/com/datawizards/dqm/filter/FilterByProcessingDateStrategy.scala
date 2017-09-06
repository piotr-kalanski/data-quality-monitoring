package com.datawizards.dqm.filter

import java.sql.Date

import org.apache.spark.sql.DataFrame

trait FilterByProcessingDateStrategy {
  def filter(input: DataFrame, processingDate: Date): DataFrame
}
