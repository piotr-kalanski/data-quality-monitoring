package com.datawizards.dqm.configuration.location

import java.util.Date

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
                              stddev: Option[Double] = None/*,
                              year: Int,
                              month: Int,
                              day: Int,
                              date: Date*/
                            )