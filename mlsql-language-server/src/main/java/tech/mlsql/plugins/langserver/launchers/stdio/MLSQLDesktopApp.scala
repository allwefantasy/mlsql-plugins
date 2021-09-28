package tech.mlsql.plugins.langserver.launchers.stdio

import streaming.core.StreamingApp
import tech.mlsql.common.utils.path.PathFun

import scala.collection.mutable.ArrayBuffer

/**
 * 26/8/2021 WilliamZhu(allwefantasy@gmail.com)
 */
object MLSQLDesktopApp {
  def main(args: Array[String]): Unit = {
    val defaultMap = arrayToMap(Array(
      "-streaming.master", "local[*]",
      "-streaming.name", "MLSQL-desktop",
      "-streaming.rest", "true",
      "-streaming.thrift", "false",
      "-streaming.platform", "spark",
      "-streaming.spark.service", "true",
      "-streaming.job.cancel", "true",
      "-streaming.datalake.path", PathFun.joinPath(".","data"),
      "-streaming.driver.port", "9003",
      "-streaming.plugin.clzznames", "tech.mlsql.plugins.ds.MLSQLExcelApp,tech.mlsql.plugins.shell.app.MLSQLShell,tech.mlsql.plugins.assert.app.MLSQLAssert"
    ))
    val extraMap = arrayToMap(args)
    StreamingApp.main( mapToArray(defaultMap ++ extraMap))
  }

  def arrayToMap(args: Array[String]): Map[String, String] = {
    val res = scala.collection.mutable.HashMap[String, String]()
    var i = 0;
    while (i < args.length) {
      res += (args(i) -> args(i + 1))
      i += 2
    }
    res.toMap
  }

  def mapToArray(args:Map[String,String]):Array[String] = {
    args.flatMap{item=>
      val (key,value) = item
      Array(key,value)
    } .toArray
  }
}

class MLSQLDesktopApp
