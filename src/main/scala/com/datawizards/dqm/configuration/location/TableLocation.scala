package com.datawizards.dqm.configuration.location

import org.apache.spark.sql.DataFrame

trait TableLocation {
  def load(): DataFrame
}
