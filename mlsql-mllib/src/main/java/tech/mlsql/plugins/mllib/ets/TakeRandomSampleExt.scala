package tech.mlsql.plugins.mllib.ets

import org.apache.spark.ml.param.Param
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.{DataFrame, SparkSession}
import streaming.dsl.mmlib._
import streaming.dsl.mmlib.algs.classfication.BaseClassification
import streaming.dsl.mmlib.algs.param.BaseParams
import streaming.dsl.mmlib.algs.{Functions, MllibFunctions}

/**
 * 27/9/2021 WilliamZhu(allwefantasy@gmail.com)
 */
class TakeRandomSampleExt(override val uid: String) extends SQLAlg
  with Functions
  with MllibFunctions
  with BaseClassification
  with PluginBaseETAuth {

  def this() = this(BaseParams.randomUID())

  override def train(df: DataFrame, path: String, params: Map[String, String]): DataFrame = {
    val _size = params.getOrElse(size.name, $(size).toString).toLong
    val _fraction = params.getOrElse(fraction.name, $(fraction).toString).toDouble

    val newdf = (_fraction, _size) match {
      case (-1, -1) =>
        df
      case (-1, s) =>
        val count = df.count()
        df.sample(Math.min(s * 1.0 / count + 0.2, 1.0)).limit(s.toInt)
      case (f, -1) =>
        df.sample(f)

      case (f, s) =>
        df.sample(Math.min(f + 0.1, 1.0)).limit(s.toInt)
    }
    if (_fraction != -1) {
      df.sample(_fraction)
    }

    newdf
  }


  override def batchPredict(df: DataFrame, path: String, params: Map[String, String]): DataFrame = {
    train(df, path, params)
  }

  override def load(sparkSession: SparkSession, path: String, params: Map[String, String]): Any = ???

  override def predict(sparkSession: SparkSession, _model: Any, name: String, params: Map[String, String]): UserDefinedFunction = ???

  override def etName: String = "__take_random_sample_operator__"

  override def modelType: ModelType = ProcessType

  override def doc: Doc = Doc(MarkDownDoc,
    """
      |
      |""".stripMargin)

  override def codeExample: Code = Code(SQLCode,
    """
      |
      |
      |""".stripMargin)

  final val fraction: Param[Double] = new Param[Double](this, name = "fraction", doc = "")
  setDefault(fraction, -1.0D)

  final val size: Param[Long] = new Param[Long](this, "size", "")
  setDefault(size, -1L)


}
