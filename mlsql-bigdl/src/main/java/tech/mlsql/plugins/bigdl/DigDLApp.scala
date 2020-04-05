package tech.mlsql.plugins.bigdl

import tech.mlsql.ets.register.ETRegister
import tech.mlsql.version.VersionCompatibility

/**
 * 5/4/2020 WilliamZhu(allwefantasy@gmail.com)
 */
class DigDLApp extends tech.mlsql.app.App with VersionCompatibility {
  override def run(args: Seq[String]): Unit = {
    ETRegister.register("ImageLoader", classOf[SQLImageLoaderExt].getName)
    ETRegister.register("MnistLoader", classOf[SQLMnistLoaderExt].getName)
    ETRegister.register("BigDLClassify", classOf[SQLBigDLClassifyExt].getName)
    ETRegister.register("LeNet5", classOf[SQLLeNet5Ext].getName)
  }

  override def supportedVersions: Seq[String] = Seq("1.5.0-SNAPSHOT", "1.5.0", "1.6.0-SNAPSHOT", "1.6.0")
}

