package tech.mlsql.plugins.canal.sink

/**
  * Created by zhuml on 2021/6/11.
  */
object BinlogConstants {

  val TS_FIELD = "___ts___"
  val DELETE_FIELD = "___delete___"
}

case class BinlogRecord(data: Array[Map[String, String]],
                        database: String,
                        es: String,
                        id: Long,
                        isDdl: Boolean,
                        mysqlType: Map[String, String],
                        old: Array[Map[String, String]],
                        pkNames: Array[String],
                        sql: String,
                        sqlType: Map[String, Int],
                        table: String,
                        ts: Long,
                        `type`: String)
