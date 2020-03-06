package tech.mlsql.plugins.app

import _root_.streaming.core.strategy.platform.{PlatformManager, SparkRuntime}
import _root_.streaming.dsl._
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import tech.mlsql.common.utils.log.Logging
import tech.mlsql.datalake.DataLake
import tech.mlsql.plugins.et.{ConnectMetaItem, ConnectPersistMeta}
import tech.mlsql.version.VersionCompatibility

/**
 * 15/1/2020 WilliamZhu(allwefantasy@gmail.com)
 */
class ConnectPersistApp extends tech.mlsql.app.App with VersionCompatibility with Logging {
  override def run(args: Seq[String]): Unit = {
    val root = runtime.sparkSession
    import root.implicits._
    import ConnectPersistApp._

    val streams = tryReadTable(root, ConnectPersistMeta.connectTableName, () => root.createDataset[ConnectMetaItem](Seq()).toDF())
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

object ConnectPersistApp {
  val DELTA_FORMAT = "org.apache.spark.sql.delta.sources.MLSQLDeltaDataSource"
  val SCHEDULER_DEPENDENCY_JOBS = "scheduler.dependency_jobs"
  val SCHEDULER_TIME_JOBS = "scheduler.time_jobs"
  val SCHEDULER_TIME_JOBS_STATUS = "scheduler.time_jobs_status"
  val SCHEDULER_LOG = "scheduler.log"

  def saveTable(spark: SparkSession, data: DataFrame, tableName: String, updateCol: Option[String], isDelete: Boolean) = {
    val dataLake = new DataLake(spark)
    require(dataLake.isEnable, "please set -streaming.datalake.path enable delta db mode")
    val finalPath = dataLake.identifyToPath(tableName)
    val writer = data.write.format(DELTA_FORMAT)
    if (updateCol.isDefined) {
      writer.option("idCols", updateCol.get)
      if (isDelete) {
        writer.option("operation", "delete")
      }

    }
    try {
      writer.mode(SaveMode.Append).save(finalPath)
    } catch {
      case e: Exception =>
    }

  }

  def tryReadTable(spark: SparkSession, table: String, empty: () => DataFrame) = {
    try {
      readTable(spark, table)
    } catch {
      case e: Exception =>
        empty()
    }
  }

  def readTable(spark: SparkSession, tableName: String) = {
    val dataLake = new DataLake(spark)
    require(dataLake.isEnable, "please set -streaming.datalake.path enable delta db mode")
    val finalPath = dataLake.identifyToPath(tableName)
    spark.read.format(DELTA_FORMAT).load(finalPath)
  }
}
