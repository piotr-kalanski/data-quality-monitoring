package com.datawizards.dqm.rules

import org.apache.spark.sql.Row

case class DictionaryRule(values: Seq[String]) extends FieldRule with AnyTypeComparator {
  override def name: String = "DICT"

  override def validate(field: String, row: Row): Boolean = {
    val value = row.getAs[Any](field).toString
    values.contains(value)
  }
}
