package com.datawizards.dqm.rules

import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.types.StructType

trait RowBuilder {
  def createRow(values: Array[Any], schema: StructType): Row = {
    new GenericRowWithSchema(values, schema)
  }
}
