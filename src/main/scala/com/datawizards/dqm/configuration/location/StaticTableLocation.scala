package com.datawizards.dqm.configuration.location

import org.apache.spark.sql.DataFrame

case class StaticTableLocation(df: DataFrame) extends TableLocation {
  override def load(): DataFrame = df
}
