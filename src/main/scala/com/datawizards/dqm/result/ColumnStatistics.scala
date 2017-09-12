package com.datawizards.dqm.result

case class ColumnStatistics (
                              tableName: String,
                              columnName: String,
                              columnType: String,
                              notMissingCount: Long,
                              rowsCount: Long,
                              percentageNotMissing: Double,
                              min: Option[Double] = None,
                              max: Option[Double] = None,
                              avg: Option[Double] = None,
                              stddev: Option[Double] = None,
                              year: Int,
                              month: Int,
                              day: Int
                            )