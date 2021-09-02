package tech.mlsql.plugins.langserver

import net.csdn.common.exception.RenderFinish
import net.csdn.common.jline.ANSI.Renderer.RenderException
import net.csdn.modules.http.DefaultRestRequest
import net.csdn.modules.mock.MockRestResponse
import streaming.rest.RestController
import tech.mlsql.autosuggest.statement.SuggestItem
import tech.mlsql.common.utils.log.Logging
import tech.mlsql.common.utils.serder.json.JSONTool

import scala.collection.JavaConverters._

/**
 * 1/9/2021 WilliamZhu(allwefantasy@gmail.com)
 */
class AutoSuggestWrapper(params: java.util.Map[String, String]) extends Logging {
  def run() = {
    try {
      params.put("executeMode", "autoSuggest")
      logInfo(JSONTool.toJsonStr(params.asScala.toMap))

      val restRequest = new DefaultRestRequest("POST", params)
      val restReponse = new MockRestResponse()
      val controller = new RestController()
      net.csdn.modules.http.RestController.enhanceApplicationController(controller, restRequest, restReponse)
      try {
        controller.script
      } catch {
        case _: RenderFinish =>
      }
      val jsonStr = restReponse.content()
      JSONTool.parseJson[List[SuggestItem]](jsonStr).asJava
    } catch {
      case e: Exception =>
        logInfo("Suggest fail", e)
        List[SuggestItem]().asJava
    }


  }
}
