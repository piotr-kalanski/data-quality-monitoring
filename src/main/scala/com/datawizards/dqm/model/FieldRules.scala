package com.datawizards.dqm.model

import com.datawizards.dqm.rules.FieldRule

case class FieldRules(field: String, rules: Seq[FieldRule])
