package tech.mlsql.plugins.ds.app

import streaming.core.datasource.MLSQLRegistry
import tech.mlsql.common.utils.classloader.ClassLoaderTool
import tech.mlsql.common.utils.log.Logging
import tech.mlsql.version.VersionCompatibility

/**
 * 1/6/2021 WilliamZhu(allwefantasy@gmail.com)
 */
class MLSQLDs extends tech.mlsql.app.App with VersionCompatibility with Logging {
  override def run(args: Seq[String]): Unit = {
    registerDS(classOf[MLSQLXml].getName)
  }


  def registerDS(name: String) = {
    val dataSource = ClassLoaderTool.classForName(name).newInstance()
    if (dataSource.isInstanceOf[MLSQLRegistry]) {
      dataSource.asInstanceOf[MLSQLRegistry].register()
    }
  }

  override def supportedVersions: Seq[String] = {
    MLSQLDs.versions
  }
}

object MLSQLDs {
  val versions = Seq(">=2.1.0")
}