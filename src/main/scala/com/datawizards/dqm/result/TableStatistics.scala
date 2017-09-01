package com.datawizards.dqm.result

import java.util.Date

case class TableStatistics (
                             tableName: String,
                             rowsCount: Long,
                             columnsCount: Int/*,
                             year: Int,
                             month: Int,
                             day: Int,
                             date: Date*/
                           )
