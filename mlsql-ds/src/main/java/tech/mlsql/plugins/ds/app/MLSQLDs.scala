package tech.mlsql.plugins.ds.app

import tech.mlsql.common.utils.log.Logging
import tech.mlsql.version.VersionCompatibility

/**
 * 1/6/2021 WilliamZhu(allwefantasy@gmail.com)
 */
class MLSQLDs extends tech.mlsql.app.App with VersionCompatibility with Logging {
  override def run(args: Seq[String]): Unit = {
    
  }


  override def supportedVersions: Seq[String] = {
    MLSQLMllib.versions
  }
}

object MLSQLMllib {
  val versions = Seq("2.1.0", "2.1.0-SNAPSHOT", "2.0.0", "2.0.1")
}