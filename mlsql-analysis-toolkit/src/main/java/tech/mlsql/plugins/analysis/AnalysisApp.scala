package tech.mlsql.plugins.analysis

import tech.mlsql.dsl.CommandCollection
import tech.mlsql.ets.register.ETRegister
import tech.mlsql.version.VersionCompatibility

/**
 * 26/4/2020 WilliamZhu(allwefantasy@gmail.com)
 */
class AnalysisApp extends tech.mlsql.app.App with VersionCompatibility {
  override def run(args: Seq[String]): Unit = {
    ETRegister.register("ApproxQuantile", classOf[ApproxQuantile].getName)
    CommandCollection.refreshCommandMapping(Map("approxQuantile" -> "ApproxQuantile"))

    ETRegister.register("DFTool", classOf[DFTool].getName)
    CommandCollection.refreshCommandMapping(Map("dataframe" -> "DFTool"))
  }

  override def supportedVersions: Seq[String] = Seq("1.5.0-SNAPSHOT", "1.5.0", "1.6.0-SNAPSHOT", "1.6.0")
}