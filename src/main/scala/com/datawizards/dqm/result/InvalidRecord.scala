package com.datawizards.dqm.result

case class InvalidRecord(
                          tableName: String,
                          columnName: String,
                          row: String,
                          value: String,
                          rule: String
                        )

