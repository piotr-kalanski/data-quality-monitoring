package com.datawizards.dqm.rules

import org.apache.spark.sql.Row

trait FieldRule extends BusinessRule {
  def name: String
  def validate(field: String, row: Row): Boolean
}
