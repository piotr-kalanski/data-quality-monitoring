package com.datawizards.dqm.rules

import org.apache.spark.sql.Row

case class RegexRule(regex: String) extends FieldRule with AnyTypeComparator {
  override def name: String = "regex"

  override def validate(field: String, row: Row): Boolean = {
    val value = row.getAs[Any](field).toString
    value.matches(regex)
  }
}
