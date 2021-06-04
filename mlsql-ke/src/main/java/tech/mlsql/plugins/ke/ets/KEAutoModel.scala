package tech.mlsql.plugins.ke.ets

import com.alibaba.fastjson.JSONObject
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.catalyst.catalog.HiveTableRelation
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.datasources.{HadoopFsRelation, LogicalRelation}
import streaming.dsl.mmlib.algs.param.WowParams
import streaming.dsl.{ConnectMeta, DBMappingKey}
import tech.mlsql.common.utils.log.Logging

class KEAutoModel(override val uid: String) extends KEAPISchedule with WowParams with Logging {

  def this() = this(Identifiable.randomUID("tech.mlsql.plugins.ets.KEAutoModel"))

  override def train(df: DataFrame, path: String, params: Map[String, String]): DataFrame = {
    val jsonObj = new JSONObject
    val split = path.split("\\.")
    val connectName = split(0)
    jsonObj.put("project", split(1))
    if (params.contains("sqls")) {
      val sqls = params("sqls").split("\\;")
      jsonObj.put("sqls", sqls)
    }else{
      throw new Exception("The sqls parameter is required")
    }
    if (params.contains("with_segment")) {
      jsonObj.put("with_segment", params("with_segment").toBoolean)
    }
    if (params.contains("with_model_online")) {
      jsonObj.put("with_model_online", params("with_model_online").toBoolean)
    }

    if(params.getOrElse("auto_add_tables","false").toBoolean){
      val spark = df.sparkSession
      val lp = df.queryExecution.analyzed
      val dataSources1 = getSourceRelation(lp)
      //collect dataSources from sqls
      val dataSources2 = parserTablesFromSqls(df.sparkSession,params("sqls").split("\\;"))

      var dataSources3:Array[String] = Array()
      if (params.get("tables").nonEmpty){
        dataSources3 = params("tables").split(",")
      }
      val dataSources = (dataSources1.keySet ++ dataSources2.keySet ++ dataSources3.toSet).toArray
      if(dataSources.nonEmpty){
        var url1 = new String
        ConnectMeta.presentThenCall(DBMappingKey("ke", connectName), options => {
          url1 = "http://" + options("host") + ":" + options("port") + "/kylin/api/tables"
        })

        val jsonObj1 = new JSONObject
        jsonObj1.put("project",split(1))
        jsonObj1.put("tables",dataSources)
        jsonObj1.put("need_sampling",false)

        sendPostAPI(df, Map(("Accept"->"application/vnd.apache.kylin-v4-public+json")), jsonObj1, url1, connectName)
      }
    }

    var url = new String
    ConnectMeta.presentThenCall(DBMappingKey("ke", connectName), options => {
      url = "http://" + options("host") + ":" + options("port") + "/kylin/api/models/model_suggestion"
    })
    sendPostAPI(df, Map(("Accept"->"application/vnd.apache.kylin-v4-public+json")), jsonObj, url, connectName)
  }

  def parserTablesFromSqls(spark:SparkSession, sqls:Array[String]): Map[String,Map[String, String]] ={
    var sourcesMaps = sqls.map(parserTablesFromSql(spark,_))
    var sources = sourcesMaps.reduce((x,y)=>x ++ y)
    sources
  }

  def parserTablesFromSql(spark:SparkSession, sql:String): Map[String,Map[String, String]] ={
    val lp = spark.sql(sql).queryExecution.analyzed
    val dataSources = getSourceRelation(lp)
    return dataSources
  }


  // 表可能会来自不同类型的数据库，不同名称的数据库。但是目前KE(4.3.3)只支持添加hive数据源，所以目前只考虑hive数据源
  def getSourceRelation(lp: LogicalPlan): Map[String,Map[String, String]] ={
//    println(lp.toString())
    var sources:Map[String,Map[String, String]] = Map()
    lp transform {
      case lr@HiveTableRelation(catalogTable, _, _) =>
        var dbName = "default"
        if(lr.tableMeta.identifier.database.nonEmpty){
          dbName = lr.tableMeta.identifier.database.get
        }
        val dbtableName = dbName + "." + lr.tableMeta.identifier.table
        sources += (dbtableName -> Map())
        lr
      case lr@LogicalRelation(hfr@HadoopFsRelation(_,_,_,_,_,_), _,catalogTable@Some(x),_) =>
        val ct = catalogTable.get
        if(ct.tableType.name.toString == "MANAGED"){
          var dbName = "default"
          if(ct.identifier.database.nonEmpty){
            dbName = ct.identifier.database.get
          }
          val dbtableName = dbName + "." + ct.identifier.table
          sources += (dbtableName -> Map())
        }
        lr
    }
    sources
  }

}