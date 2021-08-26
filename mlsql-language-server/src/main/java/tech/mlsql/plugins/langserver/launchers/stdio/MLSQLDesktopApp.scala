package tech.mlsql.plugins.langserver.launchers.stdio

import streaming.core.StreamingApp

/**
 * 26/8/2021 WilliamZhu(allwefantasy@gmail.com)
 */
object MLSQLDesktopApp {
  def main(args: Array[String]): Unit = {
    StreamingApp.main(Array(
      "-streaming.master", "local[*]",
      "-streaming.name", "MLSQL-desktop",
      "-streaming.rest", "true",
      "-streaming.thrift", "false",
      "-streaming.platform", "spark",
      "-streaming.spark.service", "true",
      "-streaming.job.cancel", "true",
      "-streaming.datalake.path", "./data/",
      "-streaming.driver.port", "9003"
    ) ++ args)
  }
}
class MLSQLDesktopApp
