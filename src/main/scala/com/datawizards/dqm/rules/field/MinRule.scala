package com.datawizards.dqm.rules.field

import org.apache.spark.sql.Row

case class MinRule(min: Any) extends FieldRule with AnyTypeComparator {
  override def name: String = "MIN"

  override def validate(field: String, row: Row): Boolean = {
    val value = row.getAs[Any](field)
    compare(value, min) >= 0
  }
}
