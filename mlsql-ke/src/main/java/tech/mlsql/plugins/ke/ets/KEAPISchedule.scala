package tech.mlsql.plugins.ke.ets

import com.alibaba.fastjson.{JSON, JSONObject}
import org.apache.commons.codec.binary.Base64
import org.apache.http.HttpEntity
import org.apache.http.client.methods._
import org.apache.http.entity.StringEntity
import org.apache.http.impl.client.HttpClients
import org.apache.http.util.EntityUtils
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.{DataFrame, SparkSession}
import streaming.dsl.mmlib.SQLAlg
import streaming.dsl.mmlib.algs.param.WowParams
import streaming.dsl.{ConnectMeta, DBMappingKey}
import tech.mlsql.common.utils.log.Logging

class KEAPISchedule(override val uid: String) extends SQLAlg with WowParams with Logging {

  def this() = this(Identifiable.randomUID("tech.mlsql.plugins.ke.ets.KEAPISchedule"))

  override def train(df: DataFrame, path: String, params: Map[String, String]): DataFrame = {
    val url = params("url")
    val connectName = path.split("\\.")(0)
    val method = params.getOrElse("method", "GET")
    if (method.equals("GET")) {
      sendGETAPI(df, params, url, connectName)
    } else if (method.equals("POST")) {
      val jsonObject = JSON.parseObject(params("body"))
      sendPostAPI(df, params, jsonObject, url, connectName)
    } else if (method.equals("PUT")) {
      val jsonObject = JSON.parseObject(params("body"))
      sendPutAPI(df, params, jsonObject, url, connectName)
    } else {
      logInfo("Other request methods are not currently supported")
      df
    }
  }

  override def load(sparkSession: SparkSession, path: String, params: Map[String, String]): Any = ???

  override def predict(sparkSession: SparkSession, _model: Any, name: String, params: Map[String, String]): UserDefinedFunction = ???

  def sendPostAPI(df: DataFrame, params: Map[String, String], jsonObj: JSONObject, url: String, connectName: String): DataFrame = {
    val httpPost = new HttpPost(url)
    setHeader(params, connectName, httpPost)
    httpPost.setEntity(new StringEntity(jsonObj.toJSONString))
    executeHTTPMethod(df, httpPost)
  }

  def sendPutAPI(df: DataFrame, params: Map[String, String], jsonObj: JSONObject, url: String, connectName: String): DataFrame = {
    val httpPut = new HttpPut(url)
    setHeader(params, connectName, httpPut)
    httpPut.setEntity(new StringEntity(jsonObj.toJSONString))
    executeHTTPMethod(df, httpPut)
  }

  def sendGETAPI(df: DataFrame, params: Map[String, String], url: String, connectName: String): DataFrame = {
    val httpGet = new HttpGet(url)
    setHeader(params, connectName, httpGet)
    executeHTTPMethod(df, httpGet)
  }

  def setHeader(params: Map[String, String], connectName: String, request: HttpRequestBase): Unit = {
    request.addHeader("Content-Type", "application/json;charset=UTF-8")
    val accept = params.getOrElse("Accept", "application/vnd.apache.kylin-v4-public+json")
    request.addHeader("Accept", accept)
    request.addHeader("Accept-Language", "en")
    var authorization = new String
    ConnectMeta.presentThenCall(DBMappingKey("ke", connectName), options => {
      authorization = options("username") + ":" + options("password")
    })
    request.addHeader("Authorization",
      "Basic " +
        new String(Base64.encodeBase64(authorization.getBytes())))
  }

  def executeHTTPMethod(df: DataFrame, request: HttpRequestBase): DataFrame = {
    val client = HttpClients.createDefault()
    val response: CloseableHttpResponse = client.execute(request)
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
