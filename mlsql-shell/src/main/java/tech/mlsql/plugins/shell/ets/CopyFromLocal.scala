package tech.mlsql.plugins.shell.ets

import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.{DataFrame, SparkSession}
import streaming.dsl.ScriptSQLExec
import streaming.dsl.auth._
import streaming.dsl.mmlib.SQLAlg
import streaming.dsl.mmlib.algs.Functions
import streaming.dsl.mmlib.algs.param.{BaseParams, WowParams}
import tech.mlsql.common.utils.serder.json.JSONTool
import tech.mlsql.dsl.auth.ETAuth
import tech.mlsql.dsl.auth.dsl.mmlib.ETMethod.ETMethod
import tech.mlsql.plugins.shell.app.MLSQLShell
import tech.mlsql.tool.HDFSOperatorV2
import tech.mlsql.version.VersionCompatibility

/**
 * 2/6/2021 WilliamZhu(allwefantasy@gmail.com)
 */
class CopyFromLocal(override val uid: String) extends SQLAlg
  with VersionCompatibility with Functions with WowParams with ETAuth {
  def this() = this(BaseParams.randomUID())

  /**
   * !copyFromLocal src dst;
   */
  override def train(df: DataFrame, path: String, params: Map[String, String]): DataFrame = {

    HDFSOperatorV2.copyToHDFS(params("src"), path, false, false)
    import df.sparkSession.implicits._
    df.sparkSession.createDataset[String](Seq().toSeq).toDF("content")
  }

  override def skipPathPrefix: Boolean = false

  override def load(sparkSession: SparkSession, path: String, params: Map[String, String]): Any = ???

  override def predict(sparkSession: SparkSession, _model: Any, name: String, params: Map[String, String]): UserDefinedFunction = ???

  override def supportedVersions: Seq[String] = MLSQLShell.versions

  override def auth(etMethod: ETMethod, path: String, params: Map[String, String]): List[TableAuthResult] = {
    val vtable = MLSQLTable(
      db = Option(DB_DEFAULT.MLSQL_SYSTEM.toString),
      table = Option("__copy_from_local__"),
      operateType = OperateType.EMPTY,
      sourceType = Option("_mlsql_"),
      tableType = TableType.SYSTEM)

    val context = ScriptSQLExec.contextGetOrForTest()
    context.execListener.getTableAuth match {
      case Some(tableAuth) =>
        tableAuth.auth(List(vtable))
      case None => List(TableAuthResult(true, ""))
    }
  }
}
