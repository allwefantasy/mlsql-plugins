package tech.mlsql.plugins.ke.ets

import com.alibaba.fastjson.{JSON, JSONObject}
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.sql.DataFrame
import streaming.dsl.{ConnectMeta, DBMappingKey}
import streaming.dsl.mmlib.algs.param.WowParams
import tech.mlsql.common.utils.log.Logging

class KEBuildSegment(override val uid: String) extends KEAPISchedule with WowParams with Logging {

  def this() = this(Identifiable.randomUID("tech.mlsql.plugins.ets.BuildSegment"))

  override def train(df: DataFrame, path: String, params: Map[String, String]): DataFrame = {
    val jsonObj = new JSONObject
    val split = path.split("\\.")
    val connectName = split(0)
    jsonObj.put("project", split(1))
    if (params.contains("start")) {
      jsonObj.put("start", params("start"))
    }
    if (params.contains("end")) {
      jsonObj.put("end", params("end"))
    }
    if (params.contains("sub_partition_values")) {
      jsonObj.put("sub_partition_values", JSON.parseArray(params("sub_partition_values")))
    }
    if (params.contains("build_all_indexes")) {
      jsonObj.put("build_all_indexes", params("build_all_indexes").toBoolean)
    }
    if (params.contains("build_all_sub_partitions")) {
      jsonObj.put("build_all_sub_partitions", params("build_all_sub_partitions").toBoolean)
    }
    if (params.contains("priority")) {
      jsonObj.put("priority", params("priority").toInt)
    }
    var url = new String
    ConnectMeta.presentThenCall(DBMappingKey("ke", connectName), options => {
      url = "http://" + options("host") + ":" + options("port") + "/kylin/api/models/" + params("model") + "/segments"
    })
    sendPostAPI(df, params, jsonObj, url, connectName)
  }
}

