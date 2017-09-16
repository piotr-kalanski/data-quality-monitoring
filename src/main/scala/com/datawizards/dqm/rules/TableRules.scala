package com.datawizards.dqm.rules

import com.datawizards.dqm.rules.trend.TableTrendRule

case class TableRules(
                       rowRules: Seq[FieldRules],
                       tableTrendRules: Seq[TableTrendRule] = Seq.empty
                     )
