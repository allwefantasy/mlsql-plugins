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
import tech.mlsql.session.{SetItem, SetSession}
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
    import df.sparkSession.implicits._
    val args = JSONTool.parseJson[List[String]](params("parameters"))

    args match {
      case List(name, "in", "__set__", msg) =>
        if (!ScriptSQLExec.context().execListener.env().contains(name)) {
          throw new RuntimeException(msg)
        }

      case List(name, "in", "__session__", msg) =>
        val envSession = new SetSession(df.sparkSession, ScriptSQLExec.context().owner)
        envSession.envTable.map { t =>
          val items = t.as[SetItem].collect().toList.map(item => (item.k, item.v)).toMap[String, String]
          if (!items.contains(name)) {
            throw new RuntimeException(msg)
          }
        }.getOrElse {
          throw new RuntimeException(msg)
        }

      case _ =>

        val tableName = args.head
        val assertString = args.last
        val expression = args.drop(1).dropRight(1).mkString(" ")


        val assertRes = try {
          tableName match {
            case "__set__" =>
              val uuid = UUID.randomUUID().toString.replaceAll("-", "")
              val variables = new mutable.HashMap[String, Any]
              val types = new mutable.HashMap[String, Any]
              variables ++= ScriptSQLExec.context().execListener.env()
              List(evaluateWithVariableTable(VariableTable(uuid, variables, types), expression))

            case "__session__" =>
              val envSession = new SetSession(df.sparkSession, ScriptSQLExec.context().owner)
              envSession.envTable.map { t =>
                val items = t.as[SetItem].collect().toList.map(item => (item.k, item.v)).toMap[String, String]
                val uuid = UUID.randomUUID().toString.replaceAll("-", "")
                val variables = new mutable.HashMap[String, Any]
                val types = new mutable.HashMap[String, Any]
                variables ++= items
                List(evaluateWithVariableTable(VariableTable(uuid, variables, types), expression))
              }.getOrElse(List[Boolean](false))

            case _ => evaluate(tableName, expression)
          }
        } catch {
          case e: org.apache.spark.sql.AnalysisException =>
            val column = "'`(.*)`'".r.findFirstIn(e.getMessage()).getOrElse("")
            throw new RuntimeException(s"${assertString} (Variable [${column}] missing)");
        }


        if (assertRes.contains(false)) {
          throw new RuntimeException(assertString)
        }

    }


    df.sparkSession.emptyDataFrame
  }


  def evaluateWithVariableTable(variableTable: VariableTable, str: String, options: Map[String, String] = Map()) = {
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

    val temptableName = UUID.randomUUID().toString.replaceAll("-", "")
    val initialVariableTable = VariableTable(temptableName, variableTable.variables, variableTable.types)

    val item = sQLGenContext.execute(exprs.map(_.asInstanceOf[Expression]), initialVariableTable)
    session.catalog.dropTempView(temptableName)
    val lit = item.asInstanceOf[Literal]
    lit.dataType match {
      case Types.Boolean => lit.value.toString.toBoolean
    }
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
    lists.toList
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