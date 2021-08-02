package tech.mlsql.plugins.canal.ets

import org.apache.spark.ml.param.Param
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.mlsql.session.MLSQLException
import org.apache.spark.sql.{DataFrame, SparkSession}
import streaming.dsl.mmlib._
import streaming.dsl.mmlib.algs.param.WowParams
import tech.mlsql.common.utils.log.Logging
import tech.mlsql.plugins.canal.sink.{BinlogWritter, DeltaSink}

/**
  * Created by zhuml on 2021/6/11.
  */
class BinlogToDelta(override val uid: String) extends SQLAlg with WowParams with Logging {

  override def train(df: DataFrame, path: String, params: Map[String, String]): DataFrame = {

    val spark = df.sparkSession

    params.get(dbTable.name)
      .map(m => set(dbTable, m)).getOrElse {
      throw new MLSQLException(s"${dbTable.name} is required")
    }
    params.get(maxTs.name)
      .map(m => set(maxTs, m)).getOrElse {
      set(maxTs, "0")
    }

    val sink = new DeltaSink(spark, $(dbTable))
    new BinlogWritter(sink, df, $(maxTs).toLong).write

    spark.emptyDataFrame
  }

  override def load(sparkSession: SparkSession, path: String, params: Map[String, String]): Any = {
    throw new RuntimeException(s"${
      getClass.getName
    } not support load function.")
  }

  override def predict(sparkSession: SparkSession, _model: Any, name: String, params: Map[String,
    String]): UserDefinedFunction = {
    throw new RuntimeException(s"${
      getClass.getName
    } not support predict function.")
  }

  override def explainParams(sparkSession: SparkSession): DataFrame = {
    _explainParams(sparkSession)
  }

  final val dbTable: Param[String] = new Param[String](this, "dbTable", "db.table")
  final val maxTs: Param[String] = new Param[String](this, "maxTs", "delta table max ts")

  override def doc: Doc = Doc(MarkDownDoc,
    """
      |BinlogToDelta CDC数据变更捕获解析同步delta模块
      |
      |```sql
      |run table as BinlogToDelta.``
      |options daTable="a.b"
      |   as t;
      |```
      |
""".stripMargin)

  override def modelType: ModelType = ProcessType

  def this() = this(WowParams.randomUID())
}


