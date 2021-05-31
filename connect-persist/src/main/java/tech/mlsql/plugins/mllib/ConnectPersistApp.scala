package tech.mlsql.plugins.mllib

import _root_.streaming.core.strategy.platform.{PlatformManager, SparkRuntime}
import _root_.streaming.dsl._
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import tech.mlsql.common.utils.log.Logging
import tech.mlsql.datalake.DataLake
import tech.mlsql.dsl.CommandCollection
import tech.mlsql.ets.register.ETRegister
import tech.mlsql.plugins.et.{ConnectMetaItem, ConnectPersistCommand, ConnectPersistMeta}
import tech.mlsql.store.DBStore
import tech.mlsql.version.VersionCompatibility

/**
 * 15/1/2020 WilliamZhu(allwefantasy@gmail.com)
 */
class ConnectPersistApp extends tech.mlsql.app.App with VersionCompatibility with Logging {
  override def run(args: Seq[String]): Unit = {
    val root = runtime.sparkSession
    import root.implicits._

    ETRegister.register("ConnectPersistCommand", classOf[ConnectPersistCommand].getName)
    CommandCollection.refreshCommandMapping(Map("connectPersist" -> "ConnectPersistCommand"))

    val streams = DBStore.store.tryReadTable(root, ConnectPersistMeta.connectTableName, () => root.createDataset[ConnectMetaItem](Seq()).toDF())
    streams.as[ConnectMetaItem].collect().foreach { item =>
      logInfo(s"load connect statement format: ${item.format} db:${item.db}")
      ConnectMeta.options(DBMappingKey(item.format, item.db), item.options)
    }
  }

  def runtime = {
    PlatformManager.getRuntime.asInstanceOf[SparkRuntime]
  }

  override def supportedVersions: Seq[String] = Seq("1.5.0-SNAPSHOT", "1.5.0", "1.6.0-SNAPSHOT", "1.6.0")
}

