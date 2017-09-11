package com.datawizards.dqm.result

case class InvalidGroup(
                       tableName: String,
                       groupName: String,
                       groupValue: Option[String],
                       rule: String,
                       year: Int,
                       month: Int,
                       day: Int
                       )
