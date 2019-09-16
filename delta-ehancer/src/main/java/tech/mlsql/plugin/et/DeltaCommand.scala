package tech.mlsql.plugin.et

import io.delta.tables.DeltaTable
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.{DataFrame, SparkSession}
import streaming.dsl.mmlib.SQLAlg
import streaming.dsl.mmlib.algs.Functions
import streaming.dsl.mmlib.algs.param.{BaseParams, WowParams}
import tech.mlsql.common.utils.path.PathFun
import tech.mlsql.common.utils.serder.json.JSONTool
import tech.mlsql.datalake.DataLake
import tech.mlsql.version.VersionCompatibility

/**
  * 2019-09-11 WilliamZhu(allwefantasy@gmail.com)
  */
class DeltaCommand(override val uid: String) extends SQLAlg with VersionCompatibility with Functions with WowParams {
  def this() = this(BaseParams.randomUID())


  override def train(df: DataFrame, path: String, params: Map[String, String]): DataFrame = {
    val spark = df.sparkSession

    def resolveRealPath(dataPath: String) = {
      val dataLake = new DataLake(spark)
      if (dataLake.isEnable) {
        dataLake.identifyToPath(dataPath)
      } else {
        PathFun(path).add(dataPath).toPath
      }
    }


    val command = JSONTool.parseJson[List[String]](params("parameters"))
    command match {
      case Seq("pruneDeletes", dataPath, howManyHoures, _*) =>
        val deltaLog = DeltaTable.forPath(spark, resolveRealPath(dataPath))
        deltaLog.vacuum(howManyHoures.toInt)
    }

  }


  override def supportedVersions: Seq[String] = {
    Seq("1.5.0-SNAPSHOT", "1.5.0")
  }

  override def batchPredict(df: DataFrame, path: String, params: Map[String, String]): DataFrame = train(df, path, params)

  override def load(sparkSession: SparkSession, path: String, params: Map[String, String]): Any = ???

  override def predict(sparkSession: SparkSession, _model: Any, name: String, params: Map[String, String]): UserDefinedFunction = ???


}
