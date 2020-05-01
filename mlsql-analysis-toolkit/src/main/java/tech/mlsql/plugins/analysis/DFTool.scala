package tech.mlsql.plugins.analysis

import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.{DataFrame, SparkSession}
import streaming.dsl.mmlib.SQLAlg
import streaming.dsl.mmlib.algs.Functions
import streaming.dsl.mmlib.algs.param.{BaseParams, WowParams}
import tech.mlsql.common.utils.serder.json.JSONTool
import tech.mlsql.version.VersionCompatibility

/**
 * 1/5/2020 WilliamZhu(allwefantasy@gmail.com)
 */
class DFTool(override val uid: String) extends SQLAlg with VersionCompatibility with Functions with WowParams {
  def this() = this(BaseParams.randomUID())

  /**
   * !dataframe build range 100 named table1;
   */
  override def train(df: DataFrame, path: String, params: Map[String, String]): DataFrame = {
    val command = JSONTool.parseJson[List[String]](params("parameters")).toArray

    val newdf = command match {
      case Array("build", "range", end, "named", table) =>
        val temp = df.sparkSession.range(end.toLong).toDF()
        temp.createOrReplaceTempView(table)
        temp
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

