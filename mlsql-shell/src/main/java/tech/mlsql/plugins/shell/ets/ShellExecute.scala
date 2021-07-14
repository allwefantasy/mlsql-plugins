package tech.mlsql.plugins.shell.ets

import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.{DataFrame, SparkSession}
import streaming.dsl.ScriptSQLExec
import streaming.dsl.auth._
import streaming.dsl.mmlib.SQLAlg
import streaming.dsl.mmlib.algs.Functions
import streaming.dsl.mmlib.algs.param.{BaseParams, WowParams}
import tech.mlsql.common.utils.serder.json.JSONTool
import tech.mlsql.common.utils.shell.ShellCommand
import tech.mlsql.dsl.auth.ETAuth
import tech.mlsql.dsl.auth.dsl.mmlib.ETMethod.ETMethod
import tech.mlsql.plugins.shell.app.MLSQLShell
import tech.mlsql.version.VersionCompatibility

import scala.collection.mutable.ArrayBuffer

/**
 * 2/6/2021 WilliamZhu(allwefantasy@gmail.com)
 */
class ShellExecute(override val uid: String) extends SQLAlg
  with VersionCompatibility with Functions with WowParams with ETAuth {
  def this() = this(BaseParams.randomUID())

  /**
   * !sh pip install pyjava;
   */
  override def train(df: DataFrame, path: String, params: Map[String, String]): DataFrame = {
    val args = JSONTool.parseJson[List[String]](params("parameters"))
    import df.sparkSession.implicits._

    args.head match {
      case "script" =>
        val res = ShellCommand.exec(args.last)
        df.sparkSession.createDataset[String](Seq(res)).toDF("content")
      case _ =>

        val process = os.proc(args).spawn()
        val result = ArrayBuffer[String]()

        var errLine = process.stderr.readLine()

        while (errLine != null) {
          logInfo(format(errLine))
          result.append(errLine)
          errLine = process.stderr.readLine()
        }


        var line = process.stdout.readLine()
        while (line != null) {
          logInfo(format(line))
          result.append(line)
          line = process.stdout.readLine()
        }

        df.sparkSession.createDataset[String](result.toSeq).toDF("content")
    }


  }

  override def skipPathPrefix: Boolean = false

  override def load(sparkSession: SparkSession, path: String, params: Map[String, String]): Any = ???

  override def predict(sparkSession: SparkSession, _model: Any, name: String, params: Map[String, String]): UserDefinedFunction = ???

  override def supportedVersions: Seq[String] = MLSQLShell.versions

  override def auth(etMethod: ETMethod, path: String, params: Map[String, String]): List[TableAuthResult] = {
    val vtable = MLSQLTable(
      db = Option(DB_DEFAULT.MLSQL_SYSTEM.toString),
      table = Option("__shell_execute__"),
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
