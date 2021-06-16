package tech.mlsql.plugins.canal.sink

import io.delta.tables.DeltaTable
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import tech.mlsql.common.utils.path.PathFun
import tech.mlsql.datalake.DataLake

/**
  * Created by zhuml on 2021/6/11.
  */
class DeltaSink(spark: SparkSession, dbTable: String) extends Sink(dbTable: String) {

  val dataLake = new DataLake(spark)

  val finalPath = if (dataLake.isEnable) {
    dataLake.identifyToPath(dbTable)
  } else {
    PathFun(dbTable).add(dbTable).toPath
  }

  override def tableLoad() = spark.read.format("delta").load(finalPath)

  override def updateSchema(df: DataFrame): Unit = {
    df.write
      .format("delta")
      .mode("overwrite")
      .option("overwriteSchema", "true")
      .save(finalPath)
  }

  override def mergeData(ds: Dataset[BinlogRecord]): Unit = {
    val records = ds.take(1)
    if (records.length > 0) {
      val record = records(0)
      val schema = tableLoad.drop(BinlogConstants.TS_FIELD).schema
      val changesDF = duplicate(ds, schema)
      mergeToDelta(changesDF, record.pkNames, BinlogConstants.DELETE_FIELD)
    }
  }

  def mergeToDelta(df: DataFrame, pkNames: Array[String], deleteField: String): Unit = {
    val deltaTable = DeltaTable.forPath(spark, finalPath)
    val condition = pkNames.map(pk => s"s.${pk} = t.${pk}").mkString(" and ")
    deltaTable.as("t")
      .merge(
        df.as("s"), condition)
      .whenMatched(s"s.${deleteField} = true")
      .delete()
      .whenMatched().updateAll()
      .whenNotMatched(s"s.${deleteField} = false").insertAll()
      .execute()
  }
}
