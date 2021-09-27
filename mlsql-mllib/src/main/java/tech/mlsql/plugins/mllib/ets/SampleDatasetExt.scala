package tech.mlsql.plugins.mllib.ets

import org.apache.spark.ml.param.{Param, StringArrayParam}
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.mlsql.session.MLSQLException
import org.apache.spark.sql.{DataFrame, SparkSession, functions => func}
import streaming.dsl.mmlib._
import streaming.dsl.mmlib.algs.classfication.BaseClassification
import streaming.dsl.mmlib.algs.param.BaseParams
import streaming.dsl.mmlib.algs.{Functions, MllibFunctions}

import scala.util.Random

/**
 * 27/9/2021 WilliamZhu(allwefantasy@gmail.com)
 */
class SampleDatasetExt(override val uid: String) extends SQLAlg
  with Functions
  with MllibFunctions
  with BaseClassification
  with PluginBaseETAuth {

  def this() = this(BaseParams.randomUID())

  override def train(df: DataFrame, path: String, params: Map[String, String]): DataFrame = {
    val session = df.sparkSession
    val _columns = params.getOrElse(columns.name, $(columns).mkString(",")).split(",").map(_.trim).filterNot(_.isEmpty)
    val _size = params.getOrElse(size.name, $(size).toString).toLong

    val _featureSize = params.getOrElse(featuresSize.name, $(featuresSize).toString).toInt
    val _labelSize = params.getOrElse(labelSize.name, $(labelSize).toString).toInt

    var newdf = session.range(_size).as("id").toDF()
    if (_columns.size == 0 || (_columns.size == 1 && _columns.head == "id")) return newdf.toDF()

    val createFeatures = func.udf(() => {
      Array.fill(_featureSize)(Random.nextFloat)
    })

    val createLabel = func.udf(() => {
      Random.nextInt(_labelSize)
    })

    _columns.filterNot(_ == "id").foreach { column =>
      column match {
        case "features" =>
          newdf = newdf.withColumn("features", createFeatures())
        case "label" =>
          newdf = newdf.withColumn("label", createLabel())
        case a: String => throw new MLSQLException(s"$a is not support")
      }
    }
    newdf
  }


  override def batchPredict(df: DataFrame, path: String, params: Map[String, String]): DataFrame = {
    train(df, path, params)
  }

  override def load(sparkSession: SparkSession, path: String, params: Map[String, String]): Any = ???

  override def predict(sparkSession: SparkSession, _model: Any, name: String, params: Map[String, String]): UserDefinedFunction = ???

  override def etName: String = "__sample_dataset_operator__"

  override def modelType: ModelType = ProcessType

  override def doc: Doc = Doc(MarkDownDoc,
    """
      |## SampleDataset
      |
      |SampleDataset is used to create sample data for Machine learning.
      |With this plugin help, the users can create a table contains id,features,label columns
      |and we can control the table size, the features size and label size.
      |
      |""".stripMargin)

  override def codeExample: Code = Code(SQLCode,
    """
      |run command as SampleDatasetExt.`` where
      |columns="id,features,label"
      |and size="100000"
      |and featuresSize="100"
      |and labelSize="2"
      |as mockData
      |;
      |
      |select * from mockData as output;
      |
      |""".stripMargin)

  final val columns: StringArrayParam = new StringArrayParam(this, "columns", "enum:id,features,label")
  setDefault(columns, Array("id"))
  final val size: Param[Long] = new Param[Long](this, "size", "the size of mock data")
  setDefault(size, 10000L)

  final val featuresSize: Param[Int] = new Param[Int](this, "featuresSize", "the size of features column")
  setDefault(featuresSize, 100)

  final val labelSize: Param[Int] = new Param[Int](this, "labelSize", "the size of label column")
  setDefault(labelSize, 2)
}
