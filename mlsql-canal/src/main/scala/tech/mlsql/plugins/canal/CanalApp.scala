package tech.mlsql.plugins.canal

import tech.mlsql.ets.register.ETRegister
import tech.mlsql.version.VersionCompatibility

/**
  * Created by zhuml on 2021/6/11.
  */
class CanalApp extends tech.mlsql.app.App with VersionCompatibility {

  override def run(args: Seq[String]): Unit = {
    ETRegister.register("BinlogToDelta", "tech.mlsql.plugins.canal.ets.BinlogToDelta")
  }

  override def supportedVersions: Seq[String] = Seq("1.6.0-SNAPSHOT")

}
