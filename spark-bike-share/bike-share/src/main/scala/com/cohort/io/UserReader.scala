package com.cohort.io

import com.cohort.conf.BikeShareConf
import org.apache.spark.internal.Logging
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.{DataFrame, SparkSession}


trait UserReader extends Logging{
  def readUserInfo(conf: BikeShareConf, spark: SparkSession): DataFrame = {

    val inputPath = conf.uniqueUserPath()

    logInfo("reading %s".format(inputPath))

    val inputUniqueUserDf: DataFrame = try {
      Some(spark.read.json(inputPath)).get
    } catch {
      case e: Exception => spark.emptyDataFrame.withColumn("user_id", lit(null: StringType))
        .withColumn("first_timestamp", lit(null:StringType))
    }

    inputUniqueUserDf
  }

}

