package tech.mlsq.streambootstrapatstartup

import _root_.streaming.dsl.ScriptSQLExec
import _root_.streaming.dsl.mmlib.SQLAlg
import _root_.streaming.dsl.mmlib.algs.Functions
import _root_.streaming.dsl.mmlib.algs.param.{BaseParams, WowParams}
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.mlsql.session.MLSQLException
import tech.mlsql.common.utils.serder.json.JSONTool
import tech.mlsql.datalake.DataLake
import tech.mlsql.job.JobManager
import tech.mlsql.store.DBStore
import tech.mlsql.version.VersionCompatibility

/**
 * 2019-09-20 WilliamZhu(allwefantasy@gmail.com)
 */
class StreamPersistCommand(override val uid: String) extends SQLAlg with VersionCompatibility with Functions with WowParams {
  def this() = this(BaseParams.randomUID())

  override def train(df: DataFrame, path: String, params: Map[String, String]): DataFrame = {
    val spark = df.sparkSession
    val dataLake = new DataLake(spark)
    require(dataLake.isEnable, "data lake should be enabled.")
    import spark.implicits._

    val command = JSONTool.parseJson[List[String]](params("parameters"))
    command match {
      case Seq("persist", streamName) =>
        JobManager.getJobInfo.filter(f => f._2.jobName == streamName).map(f => f._2).headOption match {
          case Some(item) =>
            val data = spark.createDataset(Seq(Stream(streamName, item.jobContent, item.owner, ScriptSQLExec.context().home)))
            DBStore.store.saveTable(spark, data.toDF(), StreamAppConfig.TABLE, Option("name"), false)
            DBStore.store.readTable(spark, StreamAppConfig.TABLE)
          case None => throw new MLSQLException(s"not stream ${streamName} exists")
        }
      case Seq("remove", streamName) =>
        DBStore.store.saveTable(spark, spark.createDataset[Stream](Seq(Stream(streamName, null, null, null))).toDF(), StreamAppConfig.TABLE, Option("name"), true)
        DBStore.store.readTable(spark, StreamAppConfig.TABLE)

      case Seq("list") =>
        DBStore.store.readTable(spark, StreamAppConfig.TABLE)
    }

  }


  override def supportedVersions: Seq[String] = {
    Seq("1.5.0-SNAPSHOT", "1.5.0", "1.6.0-SNAPSHOT", "1.6.0")
  }

  override def batchPredict(df: DataFrame, path: String, params: Map[String, String]): DataFrame = train(df, path, params)

  override def load(sparkSession: SparkSession, path: String, params: Map[String, String]): Any = ???

  override def predict(sparkSession: SparkSession, _model: Any, name: String, params: Map[String, String]): UserDefinedFunction = ???
}
