package com.datawizards.dqm.rules

import org.apache.spark.sql.Row

case class MinRule(min: String) extends FieldRule {
  override def name: String = "MIN"

  override def validate(field: String, row: Row): Boolean = {
    val value = row.getAs[Any](field).toString
    value >= min
  }
}
