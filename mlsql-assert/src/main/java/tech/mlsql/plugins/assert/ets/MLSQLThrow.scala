package tech.mlsql.plugins.assert.ets

import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.mlsql.session.MLSQLException
import org.apache.spark.sql.{DataFrame, SparkSession}
import streaming.dsl.auth.TableAuthResult
import streaming.dsl.mmlib.algs.Functions
import streaming.dsl.mmlib.algs.param.{BaseParams, WowParams}
import streaming.dsl.mmlib._
import tech.mlsql.dsl.auth.ETAuth
import tech.mlsql.dsl.auth.dsl.mmlib.ETMethod.ETMethod
import tech.mlsql.plugins.assert.app.MLSQLAssert
import tech.mlsql.version.VersionCompatibility

/**
 * 4/9/2021 WilliamZhu(allwefantasy@gmail.com)
 */
class MLSQLThrow(override val uid: String) extends SQLAlg
  with VersionCompatibility with Functions with WowParams with ETAuth {
  def this() = this(BaseParams.randomUID())

  override def train(df: DataFrame, path: String, params: Map[String, String]): DataFrame = {
    throw new RuntimeException(params("msg"))
  }

  override def batchPredict(df: DataFrame, path: String, params: Map[String, String]): DataFrame = train(df, path, params)

  override def skipPathPrefix: Boolean = true

  override def load(sparkSession: SparkSession, path: String, params: Map[String, String]): Any = throw new MLSQLException(s"${getClass.getName} not support register ")

  override def predict(sparkSession: SparkSession, _model: Any, name: String, params: Map[String, String]): UserDefinedFunction = throw new MLSQLException(s"${getClass.getName} not support register ")

  override def supportedVersions: Seq[String] = MLSQLAssert.versions

  override def auth(etMethod: ETMethod, path: String, params: Map[String, String]): List[TableAuthResult] = {
    List()
  }

  override def modelType: ModelType = ProcessType

  override def doc: Doc = Doc(HtmlDoc,
    """
      |
      | This ET is used to stop the execute of the script.
    """.stripMargin)


  override def codeExample: Code = Code(SQLCode,
    """
      |
      |!throw "exception msg";
      |run command as Throw.`` where msg="";
    """.stripMargin)
}
