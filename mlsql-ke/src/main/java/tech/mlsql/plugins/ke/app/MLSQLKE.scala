package tech.mlsql.plugins.ke.app

import tech.mlsql.common.utils.log.Logging
import tech.mlsql.ets.register.ETRegister
import tech.mlsql.plugins.ke.ets.KEAPISchedule
import tech.mlsql.version.VersionCompatibility

/**
 * 2/6/2021 WilliamZhu(allwefantasy@gmail.com)
 */
class MLSQLKE extends tech.mlsql.app.App with VersionCompatibility with Logging {
  override def run(args: Seq[String]): Unit = {
    ETRegister.register("KeApi", classOf[KEAPISchedule].getName)
  }


  override def supportedVersions: Seq[String] = {
    MLSQLKE.versions
  }
}

object MLSQLKE {
  val versions = Seq("2.1.0", "2.1.0-SNAPSHOT", "2.0.0", "2.0.1")
}
