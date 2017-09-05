package com.datawizards.dqm.rules

import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class RegexRuleTest extends FunSuite with RowBuilder {

  test("Validate regex") {
    val row1 = createRow(
      Array("123-123"),
      StructType(Seq(
        StructField("f1", StringType)
      ))
    )
    val row2 = createRow(
      Array("123-23"),
      StructType(Seq(
        StructField("f1", StringType)
      ))
    )
    val rule = RegexRule("""\d{3}-\d{3}""")
    assert(rule.validate("f1", row1))
    assert(!rule.validate("f1", row2))
  }

}
