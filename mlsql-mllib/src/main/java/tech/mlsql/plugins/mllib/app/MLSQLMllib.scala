package tech.mlsql.plugins.mllib.app

import tech.mlsql.common.utils.log.Logging
import tech.mlsql.ets.register.ETRegister
import tech.mlsql.plugins.mllib.ets.ClassificationEvaluator
import tech.mlsql.version.VersionCompatibility

/**
 * 31/5/2021 WilliamZhu(allwefantasy@gmail.com)
 */
class MLSQLMllib extends tech.mlsql.app.App with VersionCompatibility with Logging {
  override def run(args: Seq[String]): Unit = {
    ETRegister.register("ClassificationEvaluator", classOf[ClassificationEvaluator].getName)
  }


  override def supportedVersions: Seq[String] = {
    MLSQLMllib.versions
  }
}

object MLSQLMllib {
  val versions = Seq("2.1.0", "2.1.0-SNAPSHOT", "2.0.0")
}