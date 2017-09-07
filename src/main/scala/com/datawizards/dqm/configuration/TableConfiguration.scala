package com.datawizards.dqm.configuration

import com.datawizards.dqm.configuration.location.TableLocation
import com.datawizards.dqm.filter.FilterByProcessingDateStrategy
import com.datawizards.dqm.rules.TableRules

case class TableConfiguration(
                               location: TableLocation,
                               rules: TableRules,
                               filterByProcessingDateStrategy: Option[FilterByProcessingDateStrategy] = None,
                               groups: Seq[GroupByConfiguration] = Seq.empty
                             )
