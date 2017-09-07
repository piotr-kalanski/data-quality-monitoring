package com.datawizards.dqm.result

case class ValidationResult(
                             invalidRecords: Seq[InvalidRecord],
                             tableStatistics: TableStatistics,
                             columnsStatistics: Seq[ColumnStatistics],
                             groupByStatisticsList: Seq[GroupByStatistics] = Seq.empty
                           )
