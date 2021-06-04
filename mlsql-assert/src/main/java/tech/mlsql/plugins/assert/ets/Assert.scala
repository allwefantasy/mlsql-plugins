package tech.mlsql.plugins.assert.ets

import java.util.UUID

import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.mlsql.session.MLSQLException
import org.apache.spark.sql.{DataFrame, SparkSession}
import streaming.dsl.ScriptSQLExec
import streaming.dsl.auth.TableAuthResult
import streaming.dsl.mmlib.SQLAlg
import streaming.dsl.mmlib.algs.Functions
import streaming.dsl.mmlib.algs.param.{BaseParams, WowParams}
import tech.mlsql.common.utils.serder.json.JSONTool
import tech.mlsql.dsl.auth.ETAuth
import tech.mlsql.dsl.auth.dsl.mmlib.ETMethod.ETMethod
import tech.mlsql.ets.SQLGenContext
import tech.mlsql.lang.cmd.compile.internal.gc._
import tech.mlsql.plugins.assert.app.MLSQLAssert
import tech.mlsql.version.VersionCompatibility

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/**
 * 4/6/2021 WilliamZhu(allwefantasy@gmail.com)
 */
class Assert(override val uid: String) extends SQLAlg
  with VersionCompatibility with Functions with WowParams with ETAuth {
  def this() = this(BaseParams.randomUID())

  /**
   * !assert table ''':status=="success"'''  "all model status should be success"
   */
  override def train(df: DataFrame, path: String, params: Map[String, String]): DataFrame = {

    val args = JSONTool.parseJson[List[String]](params("parameters"))
    val tableName = args.head
    val assertString = args.last
    val assertRes = evaluate(tableName, args.drop(1).dropRight(1).mkString(" "))
    if (assertRes.filter(_ == false).length != 0) {
      throw new RuntimeException(assertString)
    }
    df
  }

  def evaluate(tableName: String, str: String, options: Map[String, String] = Map()) = {
    val session = ScriptSQLExec.context().execListener.sparkSession
    val scanner = new Scanner(str)
    val tokenizer = new Tokenizer(scanner)
    val parser = new StatementParser(tokenizer)
    val exprs = try {
      parser.parse()
    } catch {
      case e: ParserException =>
        throw new MLSQLException(s"Error in MLSQL Line:${options.getOrElse("__LINE__", "-1").toInt + 1} \n Expression:${e.getMessage}")
      case e: Exception => throw e

    }
    val sQLGenContext = new SQLGenContext(session)

    val variableTableList = rowToMaps(session.table(tableName))

    val lists = new ArrayBuffer[Boolean]()
    for (variableTable <- variableTableList) {
      val (_variables, _types) = variableTable
      val variables = new mutable.HashMap[String, Any]
      val types = new mutable.HashMap[String, Any]
      variables ++= _variables
      types ++= _types
      val temptableName = UUID.randomUUID().toString.replaceAll("-", "")
      val initialVariableTable = VariableTable(temptableName, variables, types)
      val item = sQLGenContext.execute(exprs.map(_.asInstanceOf[Expression]), initialVariableTable)
      session.catalog.dropTempView(temptableName)
      val lit = item.asInstanceOf[Literal]
      lit.dataType match {
        case Types.Boolean => lists += lit.value.toString.toBoolean
      }
    }
    lists
  }

  private def rowToMaps(df: DataFrame) = {
    val rows = df.collect().map { row =>
      val fields = df.schema.map(_.name).toSeq
      val schema = df.schema.map(_.dataType).toSeq
      (fields.zip(row.toSeq).toMap, fields.zip(schema).toMap)
    }
    rows
  }

  private def rowToMap(df: DataFrame) = {
    val row = df.collect().head.toSeq
    val fields = df.schema.map(_.name).toSeq
    val schema = df.schema.map(_.dataType).toSeq
    (fields.zip(row).toMap, fields.zip(schema).toMap)
  }

  override def skipPathPrefix: Boolean = false

  override def load(sparkSession: SparkSession, path: String, params: Map[String, String]): Any = ???

  override def predict(sparkSession: SparkSession, _model: Any, name: String, params: Map[String, String]): UserDefinedFunction = ???

  override def supportedVersions: Seq[String] = MLSQLAssert.versions

  override def auth(etMethod: ETMethod, path: String, params: Map[String, String]): List[TableAuthResult] = {
    List()
  }
}