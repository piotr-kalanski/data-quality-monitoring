package com.datawizards.dqm.configuration.location

import org.apache.spark.sql.DataFrame

case class HiveTableLocation(table: String) extends TableLocation {
  override def load(): DataFrame = ???
}
