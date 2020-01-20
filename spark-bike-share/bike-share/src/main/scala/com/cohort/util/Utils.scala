package com.cohort.util

import com.cohort.conf.BikeShareConf
import com.typesafe.config.ConfigFactory
import org.apache.spark.internal.Logging
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import scala.collection.JavaConversions._

object Utils extends Logging {
  def selectColumns(conf: BikeShareConf, sourceKey: String, inputDf:DataFrame): DataFrame = {
    val fields = getListFromConf(conf.selectColumnsConfigFile(), sourceKey).map(col)

    inputDf.select(fields: _*)
  }

  def getListFromConf(configFileName: String, confKey: String): List[String] = {
    try {
      ConfigFactory.load(configFileName).getStringList(confKey).toList
    } catch {
      case e: Exception =>
        logError(s"*** Error parsing for $confKey as List[String] from $configFileName ***\n${e.getMessage}")
        List[String]()
    }
  }

  def pathGenerator(inputParentPath: String, datePrefix: String, processDate: String): String = {
    s"$inputParentPath/$datePrefix=$processDate/"
  }

}
