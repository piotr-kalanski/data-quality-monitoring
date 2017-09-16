package com.datawizards.dqm.result

case class InvalidTableTrend(
                       tableName: String,
                       rule: String,
                       comment: String,
                       year: Int,
                       month: Int,
                       day: Int
                       )
