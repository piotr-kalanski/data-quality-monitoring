package com.datawizards.dqm.mocks

import com.datawizards.dqm.history.HistoryStatisticsReader
import com.datawizards.dqm.result.TableStatistics

class StaticHistoryStatisticsReader(history: Map[String, Seq[TableStatistics]]) extends HistoryStatisticsReader {

  override def readTableStatistics(tableName: String): Seq[TableStatistics] =
    history(tableName)

}
