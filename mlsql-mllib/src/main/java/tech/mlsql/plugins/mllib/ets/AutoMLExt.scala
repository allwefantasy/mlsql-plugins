package tech.mlsql.plugins.mllib.ets

import org.apache.spark.ml.param.Param
import org.apache.spark.sql.{DataFrame, Row, SparkSession, functions}
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.{lit, regexp_replace, when}
import streaming.dsl.ScriptSQLExec
import streaming.dsl.auth.{DB_DEFAULT, MLSQLTable, OperateType, TableAuthResult, TableType}
import streaming.dsl.mmlib.{AlgType, Code, Doc, HtmlDoc, ModelType, SQLAlg, SQLCode}
import streaming.dsl.mmlib.algs.{CodeExampleText, Functions, MllibFunctions, SQLPythonFunc}
import streaming.dsl.mmlib.algs.classfication.BaseClassification
import streaming.dsl.mmlib.algs.param.BaseParams
import tech.mlsql.common.utils.path.PathFun
import tech.mlsql.dsl.adaptor.MLMapping
import tech.mlsql.dsl.auth.ETAuth
import tech.mlsql.dsl.auth.dsl.mmlib.ETMethod.ETMethod
import tech.mlsql.ets.register.ETRegister

import java.io.File


class AutoMLExt(override val uid: String) extends SQLAlg with Functions with MllibFunctions with BaseClassification with ETAuth {

  def this() = this(BaseParams.randomUID())

  final val sortedBy: Param[String] = new Param[String](this, "sortedBy", " ...... ")

  def getAutoMLPath(path: String, algoName: String): String = {
    PathFun.joinPath(path, "__auto_ml__" + algoName)
  }

  override def train(df: DataFrame, path: String, params: Map[String, String]): DataFrame = {
    val keepVersion = params.getOrElse("keepVersion", "true").toBoolean
    setKeepVersion(keepVersion)

    val evaluateTable = params.get("evaluateTable")
    setEvaluateTable(evaluateTable.getOrElse("None"))
    val sortedKey = params.getOrElse(sortedBy.name, "f1")
    val algo_list = params.getOrElse("algos", "NaiveBayes,RandomForest")
      .split(",").map(algo => algo.stripMargin)
    val classifier_list = algo_list.map(algo_name => {
      val tempPath = getAutoMLPath(path, algo_name)
      SQLPythonFunc.incrementVersion(tempPath, keepVersion)
      val sqlAlg = MLMapping.findAlg(algo_name)
      (tempPath.split("/").last, sqlAlg.train(df, tempPath, params))
    })
    val updatedDF = classifier_list.map(obj => {
      (obj._1, obj._2.withColumn("value", regexp_replace(obj._2("value"),
        "/_model_", obj._1 + "/_model_")))
    }).map(d => {
      d._2.withColumn("value",
        when(d._2("name") === "algIndex",
          functions.concat(lit(d._1.split(AutoMLExt.pathPrefix).last), lit("."), d._2("value")))
          .otherwise(d._2("value")))
    })
    val classifier_df = updatedDF.map(obj => {
      val metrics = obj.filter(obj("name").equalTo("metrics")).select("value").collectAsList().get(0).get(0)
      val metrics_map = metrics.toString().split("\\n").map(metric => {
        val name = metric.split(":")(0)
        val value = metric.split(":")(1)
        (name, value)
      }).toMap
      val sortedValue = metrics_map.getOrElse(sortedKey, "f1")
      (sortedValue.toFloat, obj)
    }).sortBy(-1 * _._1).map(t => t._2).reduce((x, y) => x.union(y)).toDF()
    classifier_df
  }

  def getBestModelAlgoName(sparkSession: SparkSession, path: String, params: Map[String, String]): Any = {
    var newParam = params
    val algoName = params.get("algIndex") match {
      case Some(algIndex) =>
        newParam = params.updated("algIndex", algIndex.substring(algIndex.indexOf(".") + 1))
        algIndex.substring(0, algIndex.indexOf("."))
      case None =>
        val bestModelPathAmongModels = autoMLfindBestModelPath(path, params, sparkSession)
        bestModelPathAmongModels(0).split("__").last.split("/")(0)
    }
    (newParam, algoName)
  }

  override def load(sparkSession: SparkSession, path: String, params: Map[String, String]): Any = {
    val (newParam, algoName) = getBestModelAlgoName(sparkSession, path, params)
    val bestModelBasePath = getAutoMLPath(path, algoName.asInstanceOf[String])
    val bestModel = MLMapping.findAlg(algoName.asInstanceOf[String]).load(sparkSession, bestModelBasePath, newParam.asInstanceOf[Map[String, String]])
    bestModel
  }

  def autoMLfindBestModelPath(basePath: String, params: Map[String, String], sparkSession: SparkSession): Seq[String] = {
    val d = new File(basePath)
    if (!d.exists || !d.isDirectory) {
      return Seq.empty
    }
    val allETName = (MLMapping.mapping.keys ++ ETRegister.getMapping.keys).toSet
    val algo_paths = d.listFiles().filter(f => {
      val path = f.getPath.split("__").last
      f.isDirectory && f.getPath.contains(AutoMLExt.pathPrefix) && allETName.contains(path)
    }).map(_.getPath).toList
    val autoSelectByMetric = params.getOrElse("autoSelectByMetric", "f1")
    var allModels = Array[Row]()
    allModels = algo_paths.map(path => {
      val (baseModelPath, metaPath) = getBaseModelPathAndMetaPath(path, params)
      val algo_name = path.split("/").last
      (baseModelPath, metaPath, algo_name)
    }).map(paths => {
      val baseModelPath = paths._1
      val metaModelPath = paths._2
      val modelList = sparkSession.read.parquet(metaModelPath + "/0").collect()
      modelList.map(t => {
        val s = t.toSeq
        Row.fromSeq((s.take(0) :+ "/" + paths._3 + s(0).asInstanceOf[String]) ++ s.drop(1))
      })
    }).reduce((x, y) => {
      x ++ y
    })
    val algIndex = params.get("algIndex").map(f => f.toInt)
    val bestModelPath = findBestModelPath(allModels, algIndex, basePath, autoSelectByMetric)
    bestModelPath
  }

  def findBestModelPath(modelList: Array[Row], algoIndex: Option[Int], baseModelPath: String, autoSelectByMetric: String) = {
    var algIndex = algoIndex
    val bestModelPath = algIndex match {
      case Some(i) => Seq(baseModelPath + "/" + i)
      case None =>
        modelList.map { row =>
          var metric: Row = null
          val metrics = row(3).asInstanceOf[scala.collection.mutable.WrappedArray[Row]]
          if (metrics.size > 0) {
            val targeMetrics = metrics.filter(f => f.getString(0) == autoSelectByMetric)
            if (targeMetrics.size > 0) {
              metric = targeMetrics.head
            } else {
              metric = metrics.head
              logInfo(format(s"No target metric: ${autoSelectByMetric} is found, use the first metric: ${metric.getDouble(1)}"))
            }
          }
          val metricScore = if (metric == null) {
            logInfo(format("No metric is found, system  will use first model"))
            0.0
          } else {
            metric.getAs[Double](1)
          }
          // if the model path contain __auto_ml__ that means, it is trained by autoML
          var baseModelPathTmp = PathFun.joinPath(baseModelPath, row(0).asInstanceOf[String].split("/").last)
          if (row(0).asInstanceOf[String].split("/")(1).contains(AutoMLExt.pathPrefix)) {
            baseModelPathTmp = baseModelPath + row(0).asInstanceOf[String]
          }
          (metricScore, row(0).asInstanceOf[String], row(1).asInstanceOf[Int], baseModelPathTmp)
        }
          .toSeq
          .sortBy(f => f._1)(Ordering[Double].reverse)
          .take(1)
          .map(f => {
            algIndex = Option(f._3)
            val baseModelPathTmp = f._4
            baseModelPathTmp
          })
    }
    bestModelPath
  }

  def getBaseModelPathAndMetaPath(path: String, params: Map[String, String]): (String, String) = {
    val maxVersion = SQLPythonFunc.getModelVersion(path)
    val versionEnabled = maxVersion match {
      case Some(v) => true
      case None => false
    }
    val modelVersion = params.getOrElse("modelVersion", maxVersion.getOrElse(-1).toString).toInt

    val baseModelPath = if (modelVersion == -1) SQLPythonFunc.getAlgModelPath(path, versionEnabled)
    else SQLPythonFunc.getAlgModelPathWithVersion(path, modelVersion)


    val metaPath = if (modelVersion == -1) SQLPythonFunc.getAlgMetalPath(path, versionEnabled)
    else SQLPythonFunc.getAlgMetalPathWithVersion(path, modelVersion)
    (baseModelPath, metaPath)
  }

  override def predict(sparkSession: SparkSession, _model: Any, name: String, params: Map[String, String]): UserDefinedFunction = {
    predict_classification(sparkSession, _model, name)
  }

  override def explainParams(sparkSession: SparkSession): DataFrame = {
    _explainParams(sparkSession)
  }

  override def batchPredict(df: DataFrame, path: String, params: Map[String, String]): DataFrame = {
    val (newParam, algoName) = getBestModelAlgoName(df.sparkSession, path, params)
    val basModelPath = getAutoMLPath(path, algoName.asInstanceOf[String])
    MLMapping.findAlg(algoName.asInstanceOf[String]).batchPredict(df, basModelPath, newParam.asInstanceOf[Map[String, String]])
  }

  override def explainModel(sparkSession: SparkSession, path: String, params: Map[String, String]): DataFrame = {
    val (newParam, algoName) = getBestModelAlgoName(sparkSession, path, params)
    val basModelPath = getAutoMLPath(path, algoName.asInstanceOf[String])
    MLMapping.findAlg(algoName.asInstanceOf[String]).explainModel(sparkSession, basModelPath, newParam.asInstanceOf[Map[String, String]])
  }

  override def modelType: ModelType = AlgType

  override def auth(etMethod: ETMethod, path: String, params: Map[String, String]): List[TableAuthResult] = {
    val vtable = MLSQLTable(
      Option(DB_DEFAULT.MLSQL_SYSTEM.toString),
      Option("__algo_auto_ml_operator__"),
      OperateType.SELECT,
      Option("select"),
      TableType.SYSTEM)

    val context = ScriptSQLExec.contextGetOrForTest()
    context.execListener.getTableAuth match {
      case Some(tableAuth) =>
        tableAuth.auth(List(vtable))
      case None =>
        List(TableAuthResult(granted = true, ""))
    }
  }

  override def doc: Doc = Doc(HtmlDoc,
    """
      | AutoML is an extension for finding the best models among GBT, LinearRegression,LogisticRegression,
      | NaiveBayes and RandomForest classifiers.
      |
      | It only supports binary labels and sorted by custmoized performance key.
      |
      | Use "load modelParams.`AutoML` as output;"
      |
      | to check the available parameters;
      |
      | Use "load modelExample.`AutoML` as output;"
      | get example.
      |
      | If you wanna check the params of model you have trained, use this command:
      |
      | ```
      | load modelExplain.`/tmp/model` where alg="AutoML" as outout;
      | ```
      |
    """.stripMargin)

  override def codeExample: Code = Code(SQLCode, CodeExampleText.jsonStr +
    """
      |load jsonStr.`jsonStr` as data;
      |select vec_dense(features) as features ,label as label from data
      |as data1;
      |
      |-- use AutoML
      |train data1 as AutoML.`/tmp/auto_ml` where
      |
      |-- once set true,every time you run this script, MLSQL will generate new directory for you model
      |algos="LogisticRegression,NaiveBayes"
      |
      |-- specify the metric that sort the trained models e.g. F1, Accurate
      |-- once set true,every time you run this script, MLSQL will generate new directory for you model
      |and keepVersion="true"
      |and evaluateTable="data1"
      |;
    """.stripMargin)
}

object AutoMLExt {
  val pathPrefix: String = "__auto_ml__"
}