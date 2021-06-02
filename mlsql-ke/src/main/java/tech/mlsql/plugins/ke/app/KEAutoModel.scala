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

class KEAutoModel(override val uid: String) extends SQLAlg with WowParams with Logging {

  def this() = this(Identifiable.randomUID("tech.mlsql.plugins.ets.AutoModel"))

  override def load(sparkSession: SparkSession, path: String, params: Map[String, String]): Any = ???

  override def predict(sparkSession: SparkSession, _model: Any, name: String, params: Map[String, String]): UserDefinedFunction = ???

  override def train(df: DataFrame, path: String, params: Map[String, String]): DataFrame = {
    val jsonObj = new JSONObject
    jsonObj.put("project", params("project"))
    val sqls = params("sqls").split("\\;")
    jsonObj.put("sqls", sqls)
    jsonObj.put("with_segment", params("with_segment").toBoolean)
    jsonObj.put("with_model_online", params("with_model_online").toBoolean)
    val url = "http://" + params("host") + ":" + params("port") + "/kylin/api/models/model_suggestion"
    scheduleAPI(df, params, jsonObj, url)
  }

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
