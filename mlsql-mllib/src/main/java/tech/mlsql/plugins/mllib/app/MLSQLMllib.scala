package tech.mlsql.plugins.mllib.app

import tech.mlsql.common.utils.log.Logging
import tech.mlsql.dsl.CommandCollection
import tech.mlsql.ets.register.ETRegister
import tech.mlsql.plugins.mllib.ets._
import tech.mlsql.version.VersionCompatibility

/**
 * 31/5/2021 WilliamZhu(allwefantasy@gmail.com)
 */
class MLSQLMllib extends tech.mlsql.app.App with VersionCompatibility with Logging {
  override def run(args: Seq[String]): Unit = {
    ETRegister.register("ClassificationEvaluator", classOf[ClassificationEvaluator].getName)
    ETRegister.register("RegressionEvaluator", classOf[RegressionEvaluator].getName)
    ETRegister.register("AutoMLExt", classOf[AutoMLExt].getName)
    ETRegister.register("SampleDatasetExt", classOf[SampleDatasetExt].getName)
    ETRegister.register("TakeRandomSampleExt", classOf[TakeRandomSampleExt].getName)
    ETRegister.register("ColumnsExt", classOf[ColumnsExt].getName)

    // !columns drop fields from tableName;
    CommandCollection.refreshCommandMapping(Map("columns" ->
      """
        |run {3} as ColumnsExt.`` where action="{0}" and fields="{1}"
        |""".stripMargin))

  }


  override def supportedVersions: Seq[String] = {
    MLSQLMllib.versions
  }
}

object MLSQLMllib {
  val versions = Seq(">=2.0.0", "2.1.0", "2.1.0-SNAPSHOT", "2.0.0", "2.0.1")
}