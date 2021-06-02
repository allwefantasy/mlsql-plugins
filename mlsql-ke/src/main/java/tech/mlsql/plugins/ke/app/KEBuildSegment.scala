package tech.mlsql.plugins.ke.app

import com.alibaba.fastjson.{JSON, JSONObject}
import org.apache.commons.codec.binary.Base64
import org.apache.http.HttpEntity
import org.apache.http.client.methods.{CloseableHttpResponse, HttpPost}
import org.apache.http.entity.StringEntity
import org.apache.http.impl.client.HttpClients
import org.apache.http.util.EntityUtils
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.{DataFrame, SparkSession}
import tech.mlsql.common.utils.log.Logging

class KEBuildSegment(override val uid: String) extends SQLAlg with WowParams with Logging {

  def this() = this(Identifiable.randomUID("tech.mlsql.plugins.ets.BuildSegment"))

  override def train(df: DataFrame, path: String, params: Map[String, String]): DataFrame = {
    val jsonObj = new JSONObject
    jsonObj.put("project", params("project"))
    jsonObj.put("start", params("start"))
    jsonObj.put("end", params("end"))
    if (params.contains("sub_partition_values")) {
      jsonObj.put("sub_partition_values", JSON.parseArray(params("sub_partition_values")))
    } else {
      jsonObj.put("sub_partition_values", null)
    }
    if (params.contains("build_all_indexes")) {
      jsonObj.put("build_all_indexes", params("build_all_indexes").toBoolean)
    } else {
      jsonObj.put("build_all_indexes", true)
    }
    if (params.contains("build_all_sub_partitions")) {
      jsonObj.put("build_all_sub_partitions", params("build_all_sub_partitions").toBoolean)
    } else {
      jsonObj.put("build_all_sub_partitions", false)
    }
    if (params.contains("priority")) {
      jsonObj.put("priority", params("priority").toInt)
    } equals {
      jsonObj.put("priority", 3)
    }
    val url = "http://" + params("host") + ":" + params("port") + "/kylin/api/models/" + params("model") + "/segments"
    scheduleAPI(df, params, jsonObj, url)
  }

  override def load(sparkSession: SparkSession, path: String, params: Map[String, String]): Any = ???

  override def predict(sparkSession: SparkSession, _model: Any, name: String, params: Map[String, String]): UserDefinedFunction = ???

  //TODO 该方法可以提到父类中去，但是这边没找到父类 先暂时放这，代码重复
  def scheduleAPI(df: DataFrame, params: Map[String, String], jsonObj: JSONObject, url: String) = {
    val client = HttpClients.createDefault()
    val httpPost = new HttpPost(url)
    httpPost.addHeader("Content-Type", "application/json;charset=UTF-8")
    httpPost.addHeader("Accept", "application/vnd.apache.kylin-v4-public+json")
    httpPost.addHeader("Accept-Language", "en")
    httpPost.addHeader("Authorization",
      "Basic " +
        new String(Base64.encodeBase64((params("username") + ":" + params("password")).getBytes())))
    httpPost.setEntity(new StringEntity(jsonObj.toJSONString))
    val response: CloseableHttpResponse = client.execute(httpPost)
    val entity: HttpEntity = response.getEntity
    val result = EntityUtils.toString(entity, "UTF-8")
    val jsonObject = JSON.parseObject(result)
    val code = jsonObject.getString("code")
    if (code.equals("000")) {
      df.withColumn("code", lit(jsonObject.getString("code"))).
        withColumn("data", lit(jsonObject.getJSONObject("data").toJSONString)).
        withColumn("msg", lit(jsonObject.getString("msg"))).
        select("code", "data", "msg")
    } else {
      df.withColumn("code", lit(jsonObject.getString("code"))).
        withColumn("msg", lit(jsonObject.getString("msg"))).
        select("code", "msg")
    }
  }
}
