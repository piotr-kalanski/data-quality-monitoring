package com.datawizards.dqm.configuration.location

import org.apache.spark.sql.{DataFrame, SparkSession}

case class HiveTableLocation(table: String) extends TableLocation {
  private lazy val spark = SparkSession.builder().getOrCreate()

  override def load(): DataFrame =
    spark.read.table(table)

  override def tableName: String = table
}
