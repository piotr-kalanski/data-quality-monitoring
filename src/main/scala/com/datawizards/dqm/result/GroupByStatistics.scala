package com.datawizards.dqm.result

case class GroupByStatistics(
                              tableName: String,
                              groupName: String,
                              groupByFieldValue: String,
                              rowsCount: Long,
                              year: Int,
                              month: Int,
                              day: Int
                            )
