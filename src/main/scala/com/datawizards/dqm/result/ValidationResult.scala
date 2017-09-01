package com.datawizards.dqm.result

import com.datawizards.dqm.configuration.location.ColumnStatistics

case class ValidationResult(
                             invalidRecords: Seq[InvalidRecord],
                             tableStatistics: TableStatistics,
                             columnsStatistics: Seq[ColumnStatistics]
                           )
