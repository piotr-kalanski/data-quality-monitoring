package com.datawizards.dqm.history

import com.datawizards.dqm.result.TableStatistics

trait HistoryStatisticsReader {
  def readTableStatistics(tableName: String): Seq[TableStatistics]
}
