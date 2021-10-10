package tech.mlsql.plugins.mllib.ets

import org.apache.spark.ml.param.{Param, StringArrayParam}
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.{DataFrame, SparkSession}
import streaming.dsl.mmlib.algs.classfication.BaseClassification
import streaming.dsl.mmlib.algs.param.BaseParams
import streaming.dsl.mmlib.algs.{Functions, MllibFunctions}
import streaming.dsl.mmlib._

/**
 * 10/10/2021 WilliamZhu(allwefantasy@gmail.com)
 */
class ColumnsExt(override val uid: String) extends SQLAlg
  with Functions
  with MllibFunctions
  with BaseClassification
  with PluginBaseETAuth {

  def this() = this(BaseParams.randomUID())

  override def train(df: DataFrame, path: String, params: Map[String, String]): DataFrame = {
    val _action = params.getOrElse(action.name, $(action).toString)
    val _fields = params.getOrElse(fields.name, $(fields).mkString(",")).split(",")
    val dfName = params("__dfname__")
    if (_fields.length == 0) return df
    _action match {
      case "drop" | "remove" =>
        val newdf = df.drop(_fields: _*)
        newdf.createOrReplaceTempView(dfName)
        newdf
    }
  }


  override def skipOriginalDFName: Boolean = false

  override def batchPredict(df: DataFrame, path: String, params: Map[String, String]): DataFrame = {
    train(df, path, params)
  }

  override def modelType: ModelType = ProcessType

  override def doc: Doc = Doc(MarkDownDoc,
    """
      |
      |""".stripMargin)

  override def codeExample: Code = Code(SQLCode,
    """
      |select 1 as a,2 as b as mockTable;
      |!columns drop a from mockTable;
      |select * from mockTable as output;
      |""".stripMargin)


  override def load(sparkSession: SparkSession, path: String, params: Map[String, String]): Any = ???

  override def predict(sparkSession: SparkSession, _model: Any, name: String, params: Map[String, String]): UserDefinedFunction = ???

  override def etName: String = "__columns_operator__"

  final val action: Param[String] =
    new Param[String](this, name = "action", doc = "")
  setDefault(action, "drop")

  final val fields: StringArrayParam =
    new StringArrayParam(this, name = "fields", doc = "")
  setDefault(fields, Array[String]())

}
