package com.datawizards.dqm.configuration

import com.datawizards.dqm.rules.GroupRule

case class GroupByConfiguration(
                                 groupName: String,
                                 groupByFieldName: String,
                                 rules: Seq[GroupRule] = Seq.empty
                               )
