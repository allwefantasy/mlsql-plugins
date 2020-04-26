package tech.mlsql.plugins.analysis

import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.{DataFrame, SparkSession}
import streaming.dsl.ScriptSQLExec
import streaming.dsl.mmlib.SQLAlg
import streaming.dsl.mmlib.algs.Functions
import streaming.dsl.mmlib.algs.param.{BaseParams, WowParams}
import tech.mlsql.common.utils.serder.json.JSONTool
import tech.mlsql.version.VersionCompatibility

/**
 * 26/4/2020 WilliamZhu(allwefantasy@gmail.com)
 */
class ApproxQuantile(override val uid: String) extends SQLAlg with VersionCompatibility with Functions with WowParams {
  def this() = this(BaseParams.randomUID())

  override def train(df: DataFrame, path: String, params: Map[String, String]): DataFrame = {
    val command = JSONTool.parseJson[List[String]](params("parameters")).toArray

    def compute(table: String, field: String, quantile: String, error: String) = {
      df.sparkSession.table(table).stat.approxQuantile(field, Array(quantile.toDouble), error.toDouble)
    }

    var tableName: String = null

    val res = command match {
      case Array(table, field, quantile) =>
        compute(table, field, quantile, "0").head

      case Array(table, field, quantile, "valued", value) =>
        val f = compute(table, field, quantile, "0").head
        ScriptSQLExec.context().execListener.addEnv(value, f.toString)
        f
      case Array(table, field, quantile, "named", value) =>
        tableName = value
        compute(table, field, quantile, "0").head

      case Array(table, field, quantile, error) =>
        compute(table, field, quantile, error).head

      case Array(table, field, quantile, error, "valued", value) =>
        val f = compute(table, field, quantile, error).head
        ScriptSQLExec.context().execListener.addEnv(value, f.toString)
        f
      case Array(table, field, quantile, error, "named", value) =>
        tableName = value
        compute(table, field, quantile, error).head
    }

    import df.sparkSession.implicits._
    val newdf = df.sparkSession.createDataset[Double](Seq(res)).toDF("value")
    if (tableName != null) {
      newdf.createOrReplaceTempView(tableName)
    }
    newdf

  }


  override def batchPredict(df: DataFrame, path: String, params: Map[String, String]): DataFrame = train(df, path, params)

  override def load(sparkSession: SparkSession, path: String, params: Map[String, String]): Any = ???

  override def predict(sparkSession: SparkSession, _model: Any, name: String, params: Map[String, String]): UserDefinedFunction = ???

  override def supportedVersions: Seq[String] = {
    Seq("1.5.0-SNAPSHOT", "1.5.0", "1.6.0-SNAPSHOT", "1.6.0")
  }
}
