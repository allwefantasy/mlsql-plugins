package tech.mlsql.plugins.ke.ets

import com.alibaba.fastjson.JSONObject
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.sql.DataFrame
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
    }
    if (params.contains("with_segment")) {
      jsonObj.put("with_segment", params("with_segment").toBoolean)
    }
    if (params.contains("with_model_online")) {
      jsonObj.put("with_model_online", params("with_model_online").toBoolean)
    }
    var url = new String
    ConnectMeta.presentThenCall(DBMappingKey("ke", connectName), options => {
      url = "http://" + options("host") + ":" + options("port") + "/kylin/api/models/model_suggestion"
    })
    sendPostAPI(df, params, jsonObj, url, connectName)
  }
}