package tech.mlsql.plugins.bigdl

import tech.mlsql.ets.register.ETRegister
import tech.mlsql.version.VersionCompatibility

/**
 * 5/4/2020 WilliamZhu(allwefantasy@gmail.com)
 */
class BigDLApp extends tech.mlsql.app.App with VersionCompatibility {
  override def run(args: Seq[String]): Unit = {
    ETRegister.register("ImageLoaderExt", classOf[SQLImageLoaderExt].getName)
    ETRegister.register("MnistLoaderExt", classOf[SQLMnistLoaderExt].getName)
    ETRegister.register("BigDLClassifyExt", classOf[SQLBigDLClassifyExt].getName)
    ETRegister.register("LeNet5Ext", classOf[SQLLeNet5Ext].getName)
  }

  override def supportedVersions: Seq[String] = Seq("1.5.0-SNAPSHOT", "1.5.0", "1.6.0-SNAPSHOT", "1.6.0")
}

