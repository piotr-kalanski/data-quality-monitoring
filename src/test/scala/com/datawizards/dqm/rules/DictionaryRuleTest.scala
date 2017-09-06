package com.datawizards.dqm.rules

import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class DictionaryRuleTest extends FunSuite with RowBuilder {
  test("Validate dictionary") {
    val row1 = createRow(
      Array("a","b"),
      StructType(Seq(
        StructField("f1", StringType),
        StructField("f2", StringType)
      ))
    )
    val row2 = createRow(
      Array("b","1"),
      StructType(Seq(
        StructField("f1", StringType),
        StructField("f2", StringType)
      ))
    )
    val rule = DictionaryRule(Seq("a", "b", "c"))
    assert(rule.validate("f1", row1))
    assert(rule.validate("f2", row1))
    assert(rule.validate("f1", row2))
    assert(!rule.validate("f2", row2))
  }
}
