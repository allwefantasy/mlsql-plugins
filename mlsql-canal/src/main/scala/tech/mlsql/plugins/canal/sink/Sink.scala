package tech.mlsql.plugins.canal.sink

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Dataset, functions => F}
import tech.mlsql.plugins.canal.util.JacksonUtil

/**
  * Created by zhuml on 2021/6/11.
  */
abstract class Sink(val table: String) {

  def tableLoad: DataFrame

  def updateSchema(df: DataFrame)

  def mergeData(ds: Dataset[BinlogRecord])

  def addTsIfNotExsit = {
    {
      val df = tableLoad
      if (!df.schema.fieldNames.contains(BinlogConstants.TS_FIELD)) {
        updateSchema(df.withColumn(BinlogConstants.TS_FIELD, typedLit[Long](0)))
      }
    }
  }

  // duplicate binlog and parser data
  def duplicate(df: Dataset[BinlogRecord],
                schema: StructType): DataFrame = {
    import df.sparkSession.implicits._
    val schemaMap = schema.fields.map(s => s.name -> s.dataType).toMap

    val f = F.udf((dataJson: String) => {
      val dataMap = JacksonUtil.fromJson(dataJson, classOf[Map[String, String]])
        .map(data => {
          if (data._2 != null) {
            schemaMap.get(data._1) match {
              case Some(IntegerType) => (data._1, data._2.toInt)
              case Some(LongType) => (data._1, data._2.toLong)
              case Some(DoubleType) => (data._1, data._2.toDouble)
              case Some(FloatType) => (data._1, data._2.toFloat)
              case _ => data
            }
          } else {
            data
          }
        })
      JacksonUtil.toJson(dataMap)
    })

    df.flatMap(r => {
      r.data.map(data => {
        (r.pkNames.map(data.get(_)), (r.ts, r.`type`, JacksonUtil.toJson(data)))
      })
    }).groupBy("_1").agg(max("_2").as("latest"))
      .withColumn(("data"), f(F.col("latest._3")))
      .select(from_json($"data", schema).as("data"), $"latest._1".as(BinlogConstants.TS_FIELD), $"latest._2".as(BinlogConstants.DELETE_FIELD))
      .selectExpr("data.*", s"${BinlogConstants.TS_FIELD}", s"if(${BinlogConstants.DELETE_FIELD}='DELETE',true,false) as ${BinlogConstants.DELETE_FIELD}")
  }
}
