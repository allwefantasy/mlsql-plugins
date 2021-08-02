package tech.mlsql.plugins.canal.util

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule

import scala.util.control.NonFatal

object JacksonUtil {

  private val _mapper = new ObjectMapper()
  _mapper.registerModule(DefaultScalaModule)

  def toJson[T](obj: T): String = {
    _mapper.writeValueAsString(obj)
  }

  def fromJson[T](json: String, `class`: Class[T]): T = {
    try {
      _mapper.readValue(json, `class`)
    } catch {
      case NonFatal(e) =>
        null.asInstanceOf[T]
    }
  }

  def prettyPrint[T](obj: T): String = {
    _mapper.writerWithDefaultPrettyPrinter().writeValueAsString(obj)
  }

}
