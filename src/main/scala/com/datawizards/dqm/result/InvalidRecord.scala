package com.datawizards.dqm.result

import java.sql.Date

case class InvalidRecord(
                          tableName: String,
                          columnName: String,
                          row: String,
                          value: String,
                          rule: String,
                          year: Int,
                          month: Int,
                          day: Int,
                          date: Date
                        )

