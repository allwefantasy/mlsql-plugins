package tech.mlsql.plugins.langserver

import tech.mlsql.autosuggest.app.AutoSuggestController
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
      val suggest = new AutoSuggestController();
      val jsonStr = suggest.run(params.asScala.toMap)
      JSONTool.parseJson[List[SuggestItem]](jsonStr).asJava
    } catch {
      case e: Exception =>
        logInfo("Suggest fail")
        List[SuggestItem]().asJava
    }


  }
}
