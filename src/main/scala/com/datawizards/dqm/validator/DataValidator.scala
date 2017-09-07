package com.datawizards.dqm.validator

import java.sql.Date

import com.datawizards.dqm.configuration.{GroupByConfiguration, TableConfiguration}
import com.datawizards.dqm.result._
import com.datawizards.dqm.rules.TableRules
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Column, DataFrame, Row}

import scala.util.parsing.json.JSONObject

object DataValidator {

  private val countColumn = "count"

  def validate(tableConfiguration: TableConfiguration, processingDate: Date): ValidationResult = {
    val tableLocation = tableConfiguration.location
    val tableRules = tableConfiguration.rules
    val filterByProcessingDateStrategy = tableConfiguration.filterByProcessingDateStrategy
    val input = tableLocation.load()
    val df = if(filterByProcessingDateStrategy.isDefined) filterByProcessingDateStrategy.get.filter(input, processingDate) else input
    val fields = getDataFrameFields(df)
    val aggregate = aggregateDataFrame(df, fields)
    val rowsCount = calculateRowsCount(aggregate)
    val tableName = tableLocation.tableName
    val localDate = processingDate.toLocalDate
    val processingYear = localDate.getYear
    val processingMonth = localDate.getMonthValue
    val processingDay = localDate.getDayOfMonth
    ValidationResult(
      invalidRecords = calculateInvalidRecords(df, tableName, tableRules, processingYear, processingMonth, processingDay),
      tableStatistics = calculateTableStatistics(df, tableName, rowsCount, processingYear, processingMonth, processingDay),
      columnsStatistics = calculateColumnStatistics(tableName, rowsCount, fields, aggregate, processingYear, processingMonth, processingDay),
      groupByStatisticsList = calculateGroupByStatistics(df, tableConfiguration.groups, tableName, processingYear, processingMonth, processingDay)
    )
  }

  private def calculateInvalidRecords(input: DataFrame, tableName: String, tableRules: TableRules, processingYear: Int, processingMonth: Int, processingDay: Int): Array[InvalidRecord] = {
    val spark = input.sparkSession
    import spark.implicits._

    input.flatMap{row =>
      tableRules.rowRules.flatMap{fieldRules => {
        val field = fieldRules.field

        fieldRules
          .rules
          .withFilter(fr => !fr.validate(field, row))
          .map{fr =>
            val fieldValue = row.getAs[Any](field)
            val values = row.getValuesMap[Any](row.schema.fieldNames).mapValues(v => if(v == null) "null" else v)
            InvalidRecord(
              tableName = tableName,
              columnName = field,
              row = JSONObject(values).toString(),
              value = if(fieldValue == null) "null" else fieldValue.toString,
              rule = fr.name,
              year = processingYear,
              month = processingMonth,
              day = processingDay
            )
          }
      }}
    }.collect()
  }

  private def calculateTableStatistics(df: DataFrame, tableName: String, rowsCount: Long, processingYear: Int, processingMonth: Int, processingDay: Int): TableStatistics = {
    TableStatistics(
      tableName = tableName,
      rowsCount = rowsCount,
      columnsCount = calculateColumnsCount(df),
      year = processingYear,
      month = processingMonth,
      day = processingDay
    )
  }

  private def aggregateDataFrame(df: DataFrame, fields: Seq[StructField]): Row = {
    val exprs = buildAggregateExpressions(fields)
    df
      .agg(count("*").alias(countColumn), exprs:_*)
      .collect()
      .head
  }

  private def buildAggregateExpressions(fields: Seq[StructField]): Seq[Column] = {
    fields
      .flatMap(f => {
        val columnName = f.name
        val numericFieldAggregations = if(isNumericType(f.dataType))
          List(
            min(columnName).alias(minForColumnName(columnName))
            , max(columnName).alias(maxForColumnName(columnName))
            , avg(columnName).alias(avgForColumnName(columnName))
            , stddev(columnName).alias(stdDevForColumnName(columnName))
          )
        else
          Nil

        count(columnName).alias(countForColumnName(columnName)) :: numericFieldAggregations
      })
  }

  private def getDataFrameFields(df: DataFrame): Seq[StructField] =
    df.schema.fields

  private def calculateColumnsCount(df: DataFrame): Int = {
    df.columns.length
  }

  private def countForColumnName(c: String) = c + "_count"
  private def minForColumnName(c: String) = c + "_min"
  private def maxForColumnName(c: String) = c + "_max"
  private def avgForColumnName(c: String) = c + "_avg"
  private def stdDevForColumnName(c: String) = c + "_stddev"

  private def calculateRowsCount(aggregate: Row): Long =
    aggregate.getAs[Long](countColumn)

  private def calculateColumnStatistics(tableName: String, rowsCount: Long, fields: Seq[StructField], aggregate: Row, processingYear: Int, processingMonth: Int, processingDay: Int): Seq[ColumnStatistics] = {
    var columnIndex = -1

    fields
      .map(f => {
        columnIndex += 1
        val columnName = getColumnName(f)
        val columnType = getColumnType(f)
        val min = getAggregateValueIfNumericField(aggregate, minForColumnName(columnName), columnType)
        val max = getAggregateValueIfNumericField(aggregate, maxForColumnName(columnName), columnType)
        val notMissingCount = aggregate.getAs[Long](countForColumnName(columnName))

        ColumnStatistics(
          tableName = tableName,
          columnName = columnName,
          columnType = f.dataType.toString,
          notMissingCount = notMissingCount,
          rowsCount = rowsCount,
          percentageNotMissing = 1.0*notMissingCount/rowsCount,
          min = min,
          max = max,
          avg = getAggregateValueIfNumericField(aggregate, avgForColumnName(columnName), columnType),
          stddev = getAggregateValueIfNumericField(aggregate, stdDevForColumnName(columnName), columnType),
          year = processingYear,
          month = processingMonth,
          day = processingDay
        )

      })
  }

  private def getAggregateValueIfNumericField(aggregate: Row, columnName: String, columnType: DataType): Option[Double] =
    if(isNumericType(columnType)) Some(castToDouble(aggregate.getAs[Double](columnName)))
    else None

  private def getColumnType(f: StructField): DataType = f.dataType

  private def getColumnName(f: StructField): String = f.name

  private def isNumericType(columnType: DataType): Boolean =
    columnType.equals(IntegerType) || columnType.equals(LongType) || columnType.equals(DoubleType)

  private def isNumericType(field: StructField): Boolean = isNumericType(field.dataType)

  private def castToDouble(v: Any): Double = v match {
    case d: Double => d
    case b: Byte => b.toDouble
    case s: Short => s.toDouble
    case i: Int => i.toDouble
    case j: Long => j.toDouble
    case f: Float => f.toDouble
    case Some(d: Double) => d
    case Some(b: Byte) => b.toDouble
    case Some(s: Short) => s.toDouble
    case Some(i: Int) => i.toDouble
    case Some(j: Long) => j.toDouble
    case Some(f: Float) => f.toDouble
    case _ => Double.NaN
  }

  private def calculateGroupByStatistics(df: DataFrame, groups: Seq[GroupByConfiguration], tableName: String, processingYear: Int, processingMonth: Int, processingDay: Int): scala.Seq[GroupByStatistics] = {
    for {
      group <- groups
      (groupByFieldValue, rowsCount) <- df.groupBy(group.groupByFieldName).count().collect().map(r => r.getAs[String](group.groupByFieldName) -> r.getAs[Long]("count"))
    }
      yield GroupByStatistics(
        tableName = tableName,
        groupName = group.groupName,
        groupByFieldValue = groupByFieldValue,
        rowsCount = rowsCount,
        year = processingYear,
        month = processingMonth,
        day = processingDay
      )
  }
}
