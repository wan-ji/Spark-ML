package com.cohort.process

import com.cohort.conf.BikeShareConf
import com.cohort.io.{BikeShareTripReader, UserReader}
import com.cohort.util.Utils
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.functions._

object UserProcess extends Logging with UserReader with BikeShareTripReader {

  val spark = SparkSession
    .builder()
    .appName("Unique-users")
    .getOrCreate()

  def main(args: Array[String]): Unit = {
    val conf = new BikeShareConf(args)

    val bikeSharePath =  Utils.pathGenerator(conf.inputBikeSharePath(),
      conf.datePrefix(), conf.processDate())

    uniqueUser(conf.uniqueUserPath(), bikeSharePath, conf)

  }

  def uniqueUser(uniqueUsersPath: String, inputBikeSharePath: String, conf: BikeShareConf): Unit = {
    val inputUniqueUsersDf = readUserInfo(conf, spark)
    val inputBikeShareDf = readBikeShareTrip(conf, spark)

    val users = Utils.selectColumns(conf, "bike.unique.user", inputBikeShareDf)
      .withColumn("first_timestamp", col("start_timestamp"))
      .drop(col("start_timestamp"))

    val uniqueUserDf = inputUniqueUsersDf.union(users)
      .groupBy("user_id")
      .agg(min("first_timestamp").as("first_timestamp"))

    uniqueUserDf.distinct().coalesce(1).write.mode(SaveMode.Overwrite).json(uniqueUsersPath)

  }

}
