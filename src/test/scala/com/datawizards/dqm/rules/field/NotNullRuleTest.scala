package com.datawizards.dqm.rules.field

import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
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
    val rule = NotNullRule
    assert(rule.validate("f1", row))
    assert(!rule.validate("f2", row))
    assert(rule.validate("f3", row))
  }
}
