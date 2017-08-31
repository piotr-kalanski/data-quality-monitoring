package com.datawizards.dqm.rules

import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.scalatest.FunSuite

class MinRuleTest extends FunSuite with RowBuilder {
  test("Validate min") {
    val row1 = createRow(
      Array("1", -1),
      StructType(Seq(
        StructField("f1", StringType),
        StructField("f2", IntegerType)
      ))
    )
    val row2 = createRow(
      Array("-1", 1),
      StructType(Seq(
        StructField("f1", StringType),
        StructField("f2", IntegerType)
      ))
    )
    val rule = MinRule("0")
    assert(rule.validate("f1", row1))
    assert(!rule.validate("f2", row1))
    assert(!rule.validate("f1", row2))
    assert(rule.validate("f2", row2))
  }
}
