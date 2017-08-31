package com.datawizards.dqm.rules

import org.apache.spark.sql.Row

case class MaxRule(max: Any) extends FieldRule with AnyTypeComparator {
  override def name: String = "MAX"

  override def validate(field: String, row: Row): Boolean = {
    val value = row.getAs[Any](field)
    compare(value, max) <= 0
  }
}
