package com.datawizards.dqm.mocks

import com.datawizards.dqm.history.HistoryStatisticsReader
import com.datawizards.dqm.result.TableStatistics

object EmptyHistoryStatisticsReader extends HistoryStatisticsReader {

  override def readTableStatistics(tableName: String): Seq[TableStatistics] =
    Seq.empty

}
