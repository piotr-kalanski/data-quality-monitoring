package com.datawizards.dqm.rules

import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class MaxRuleTest extends FunSuite with RowBuilder {
  test("Validate max") {
    val row1 = createRow(
      Array("abc", 11),
      StructType(Seq(
        StructField("f1", StringType),
        StructField("f2", IntegerType)
      ))
    )
    val row2 = createRow(
      Array("ad", 9),
      StructType(Seq(
        StructField("f1", StringType),
        StructField("f2", IntegerType)
      ))
    )
    val ruleString = MaxRule("ac")
    val ruleInt = MaxRule(10)
    assert(ruleString.validate("f1", row1))
    assert(!ruleInt.validate("f2", row1))
    assert(!ruleString.validate("f1", row2))
    assert(ruleInt.validate("f2", row2))
  }
}
