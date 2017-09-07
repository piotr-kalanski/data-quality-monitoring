package com.datawizards.dqm.result

case class TableStatistics (
                             tableName: String,
                             rowsCount: Long,
                             columnsCount: Int,
                             year: Int,
                             month: Int,
                             day: Int
                           )
