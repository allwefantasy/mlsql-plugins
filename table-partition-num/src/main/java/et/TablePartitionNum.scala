package tech.mlsql.plugins.et

import org.apache.spark.ml.param.{IntParam, Param}
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import streaming.dsl.auth.TableAuthResult
import streaming.dsl.mmlib._
import streaming.dsl.mmlib.algs.Functions
import streaming.dsl.mmlib.algs.param.{BaseParams, WowParams}
import tech.mlsql.dsl.auth.ETAuth
import tech.mlsql.dsl.auth.dsl.mmlib.ETMethod.ETMethod
import tech.mlsql.version.VersionCompatibility


class TablePartitionNum(override val uid: String) extends SQLAlg with VersionCompatibility with Functions with WowParams with ETAuth {
  def this() = this(BaseParams.randomUID())

  override def train(df: DataFrame, path: String, params: Map[String, String]): DataFrame = {
    params.get(showPartitionMsg.name).map { item =>
      set(showPartitionMsg, item.toInt)
      item
    }.getOrElse {
      set(showPartitionMsg, 0)
    }

    $(showPartitionMsg) match {
      case 1 =>
        val partitionMsg = df.rdd.mapPartitionsWithIndex{
          (partid, iter) => {
            var part_map = scala.collection.mutable.Map[String, Int]()
            var part_name = "partition_" + partid
            part_map(part_name) = iter.length
            part_map.iterator
          }
        }
        val columns = Array("partitionNum", "partitionMsg")
        val structField = columns.map(x => StructField(x, StringType, nullable = true))
        val schema = new StructType(structField)
        val rowRDD = df.sparkSession.sparkContext
          .parallelize(Seq(Row(df.rdd.getNumPartitions.toString, partitionMsg.take(df.rdd.getNumPartitions).mkString(", "))))
        df.sparkSession.createDataFrame(rowRDD, schema)
      case _ =>
        val columns = Array("partitionNum")
        val structField = columns.map(x => StructField(x, StringType, nullable = true))
        val schema = new StructType(structField)
        val rowRDD = df.sparkSession.sparkContext.parallelize(Seq(Row(df.rdd.getNumPartitions.toString)))
        df.sparkSession.createDataFrame(rowRDD, schema)
    }
  }

  override def auth(etMethod: ETMethod, path: String, params: Map[String, String]): List[TableAuthResult] = {
    List()
  }

  override def supportedVersions: Seq[String] = {
    Seq("1.5.0-SNAPSHOT", "1.5.0", "1.6.0-SNAPSHOT", "1.6.0")
  }


  override def doc: Doc = Doc(MarkDownDoc,
    s"""
       |
    """.stripMargin)


  override def codeExample: Code = Code(SQLCode,
    """
      |
    """.stripMargin)

  override def batchPredict(df: DataFrame, path: String, params: Map[String, String]): DataFrame = train(df, path, params)

  override def load(sparkSession: SparkSession, path: String, params: Map[String, String]): Any = ???

  override def predict(sparkSession: SparkSession, _model: Any, name: String, params: Map[String, String]): UserDefinedFunction = ???

  override def explainParams(sparkSession: SparkSession): DataFrame = _explainParams(sparkSession)
  final val showPartitionMsg: IntParam = new IntParam(this, "showPartitionMsg",
    "")
}
