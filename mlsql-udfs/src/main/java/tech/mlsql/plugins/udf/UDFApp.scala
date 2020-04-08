package tech.mlsql.plugins.udf

import streaming.core.strategy.platform.{PlatformManager, SparkRuntime}
import tech.mlsql.version.VersionCompatibility

/**
 * 8/4/2020 WilliamZhu(allwefantasy@gmail.com)
 */
class UDFApp extends tech.mlsql.app.App with VersionCompatibility {
  override def run(args: Seq[String]): Unit = {
    val runtime = PlatformManager.getRuntime.asInstanceOf[SparkRuntime]
    runtime.registerUDF("tech.mlsql.plugins.udf.DateUDF")
  }

  override def supportedVersions: Seq[String] = Seq("1.5.0-SNAPSHOT", "1.5.0", "1.6.0-SNAPSHOT", "1.6.0")
}
