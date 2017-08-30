package com.datawizards.dqm.rules

import org.apache.spark.sql.types.{StructField, StructType, StringType}
import org.scalatest.FunSuite

class NotNullRuleTest extends FunSuite with RowBuilder {
  test("Validate null") {
    val row = createRow(
      Array("not empty", null, ""),
      StructType(Seq(
        StructField("f1", StringType),
        StructField("f2", StringType),
        StructField("f3", StringType)
      ))
    )
    val rule = new NotNullRule()
    assert(rule.validate("f1", row))
    assert(!rule.validate("f2", row))
    assert(rule.validate("f3", row))
  }
}
