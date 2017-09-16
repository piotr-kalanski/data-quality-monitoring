package com.datawizards.dqm.rules.field

import com.datawizards.dqm.rules.BusinessRule
import org.apache.spark.sql.Row

trait FieldRule extends BusinessRule {
  def validate(field: String, row: Row): Boolean
}
