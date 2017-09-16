package com.datawizards.dqm.configuration

import com.datawizards.dqm.rules.group.GroupRule

case class GroupByConfiguration(
                                 groupName: String,
                                 groupByFieldName: String,
                                 rules: Seq[GroupRule] = Seq.empty
                               )
