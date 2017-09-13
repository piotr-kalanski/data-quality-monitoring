package com.datawizards.dqm.rules
import org.apache.spark.sql.Row

case object NotNullRule extends FieldRule {
  override def name: String = "NOT NULL"

  override def validate(field: String, row: Row): Boolean = {
    val value = row.getAs[Any](field)
    value != null
  }
}
