package tech.mlsql.plugins.ds.app

import org.apache.spark.sql.SparkSession
import streaming.core.datasource._
import streaming.dsl.ScriptSQLExec
import streaming.dsl.mmlib.algs.param.{BaseParams, WowParams}
import tech.mlsql.version.VersionCompatibility

/**
 * 29/9/2021 WilliamZhu(allwefantasy@gmail.com)
 */
class MLSQLXml(override val uid: String)
  extends MLSQLBaseFileSource
    with WowParams with VersionCompatibility {
  def this() = this(BaseParams.randomUID())

  override def sourceInfo(config: DataAuthConfig): SourceInfo = {
    val context = ScriptSQLExec.contextGetOrForTest()
    val owner = config.config.get("owner").getOrElse(context.owner)
    SourceInfo(shortFormat, "", resourceRealPath(context.execListener, Option(owner), config.path))
  }

  override def explainParams(spark: SparkSession) = {
    _explainParams(spark)
  }

  override def register(): Unit = {
    DataSourceRegistry.register(MLSQLDataSourceKey(fullFormat, MLSQLSparkDataSourceType), this)
    DataSourceRegistry.register(MLSQLDataSourceKey(shortFormat, MLSQLSparkDataSourceType), this)
  }

  override def fullFormat: String = "com.databricks.spark.xml"

  override def shortFormat: String = "xml"

  override def supportedVersions: Seq[String] = {
    MLSQLDs.versions
  }
}