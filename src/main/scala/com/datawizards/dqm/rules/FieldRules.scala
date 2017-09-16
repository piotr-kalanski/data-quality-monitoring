package com.datawizards.dqm.rules

import com.datawizards.dqm.rules.field.FieldRule

case class FieldRules(field: String, rules: Seq[FieldRule])
