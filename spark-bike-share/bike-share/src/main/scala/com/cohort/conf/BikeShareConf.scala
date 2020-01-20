package com.cohort.conf

import org.rogach.scallop.{ScallopConf, ScallopOption}

class BikeShareConf(args: Seq[String]) extends ScallopConf(args) with Serializable {

  val selectColumnsConfigFile: ScallopOption[String] =
    opt[String]("select.columns.config",
      descr="name of columns that you want to select",
      required=false,
      default=Option("select-columns"))

  val bikeTripKey: ScallopOption[String] =
    opt[String]("bike.trip.key",
      descr = "bike trip path key",
      required = false,
      default = Option("bike-trips"))

  val env: ScallopOption[String] =
    opt[String]("env",
      descr = "env name that job is running on (test, stage, prod)",
      required = false,
      default = Option("stage"))

  val inputBikeSharePath: ScallopOption[String] =
    opt[String]("input.bike.path",
      descr = "path to bike share data",
      required = false,
      default = env() match{
        case "test" => Option("gs://spark-bikeshare/bike-trips")
        case "stage" => Option("gs://spark-bikeshare/bike-trips")
        case "prod" => Option("gs://spark-bikeshare/bike-trips")
        case _ => None
          throw new Exception(s"env error, env name can either be test, prod, \ncannot be ${env()}")
      }
    )

  val inputMetaDataPath:  ScallopOption[String] =
    opt[String]("input.meta.path",
      descr = "input meta data parent path",
      required = false,
      default = env() match{
        case "test" => Option("gs://spark-bikeshare/bike-bike-station-info.json")
        case "stage" => Option("gs://spark-bikeshare/bike-bike-station-info.json")
        case "prod" => Option("gs://spark-bikeshare/bike-bike-station-info.json")
        case _ => None
          throw new Exception(s"env error, env name can either be test, prod, \ncannot be ${env()}")
      }
    )

  val outputDataPath: ScallopOption[String] =
    opt[String]("output.data.path",
      descr = "output data parent path",
      required = false,
      default = env() match{
        case "test" => Option("gs://spark-bikeshare/output")
        case "stage" => Option("gs://spark-bikeshare/output")
        case "prod" => Option("gs://spark-bikeshare/output")
        case _ => None
          throw new Exception(s"env error, env name can either be test, prod, \ncannot be ${env()}")
      }
    )

  val datePrefix: ScallopOption[String] =
    opt[String]("date.prefix",
      descr="date prefix for path",
      required = false,
      default=Option("start_date"))

  val processDate : ScallopOption[String] =
    opt[String]("process.date",
      descr="date to process, in YYYY-MM-DD format",
      required = true)

  val uniqueUserPath: ScallopOption[String] =
    opt[String]("unique.user.path",
      descr = "path to save unique user",
      required = false,
      default = Option("gs://spark-bikeshare/unique_user")
    )

  val dayAgo: ScallopOption[Int] =
    opt[Int]("day.ago",
      descr = "how many days ago you are going to overwrite",
      required = false,
      default = Option(1)
    )

  verify()


}
