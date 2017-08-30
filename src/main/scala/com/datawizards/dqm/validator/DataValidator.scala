package com.datawizards.dqm.validator

import com.datawizards.dqm.result.{InvalidRecord, ValidationResult}
import com.datawizards.dqm.rules.TableRules
import org.apache.spark.sql.DataFrame

import scala.util.parsing.json.JSONObject

object DataValidator {
  def validate(input: DataFrame, tableRules: TableRules): ValidationResult = {
    val spark = input.sparkSession
    import spark.implicits._

    val invalidRecords = input.flatMap{row =>
      tableRules.rowRules.flatMap{fieldRules => {
       val field = fieldRules.field

       fieldRules
         .rules
         .withFilter(fr => !fr.validate(field, row))
         .map{fr =>
           val fieldValue = row.getAs[Any](field)
           val values = row.getValuesMap[Any](row.schema.fieldNames).mapValues(v => if(v == null) "null" else v)
           InvalidRecord(
             JSONObject(values).toString(),
             if(fieldValue == null) "null" else fieldValue.toString,
             fr.name
           )
         }
      }}
    }.collect()

    ValidationResult(invalidRecords)
  }

}
