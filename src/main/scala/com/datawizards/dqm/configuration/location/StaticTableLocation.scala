package com.datawizards.dqm.configuration.location

import org.apache.spark.sql.DataFrame

case class StaticTableLocation(df: DataFrame, table: String) extends TableLocation {
  override def load(): DataFrame = df
  override def tableName: String = table
}
