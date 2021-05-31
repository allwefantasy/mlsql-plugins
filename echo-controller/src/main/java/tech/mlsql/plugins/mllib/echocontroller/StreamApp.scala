package tech.mlsql.plugins.mllib.echocontroller

import tech.mlsql.app.CustomController
import tech.mlsql.common.utils.serder.json.JSONTool
import tech.mlsql.runtime.AppRuntimeStore
import tech.mlsql.version.VersionCompatibility

/**
 * 7/11/2019 WilliamZhu(allwefantasy@gmail.com)
 */
class StreamApp extends tech.mlsql.app.App with VersionCompatibility {
  override def run(args: Seq[String]): Unit = {
    AppRuntimeStore.store.registerController("echo", classOf[EchoController].getName)
  }

  override def supportedVersions: Seq[String] = Seq("1.5.0-SNAPSHOT", "1.5.0", "1.6.0-SNAPSHOT", "1.6.0")
}

class EchoController extends CustomController {
  override def run(params: Map[String, String]): String = {
    JSONTool.toJsonStr(List(params("sql")))
  }
}
