package tech.mlsql.plugins.canal.sink

import org.apache.spark.sql.DataFrame
import tech.mlsql.plugins.canal.mysql.statement.DDLStatementParser
import tech.mlsql.plugins.canal.util.JacksonUtil

/**
  * Created by zhuml on 2021/6/11.
  */
class BinlogWritter(@transient sink: Sink, df: DataFrame, maxTs: Long) extends Serializable {

  val spark = df.sparkSession

  def write = {
    sink.addTsIfNotExsit
    val filterDF = filter()
    //segment merge by ddl
    val ddls = filterDF.filter(r => r.isDdl && Array("ALTER", "TRUNCATE").contains(r.`type`)).collect()
    val dmlDS = filterDF.filter(r => !r.isDdl && Array("INSERT", "UPDATE", "DELETE").contains(r.`type`.toUpperCase))
    var tsMin = 0L
    var tsMax = 0L
    ddls.foreach(ddl => {
      val ddlParser = new DDLStatementParser(sink.tableLoad, ddl.sql)
      ddlParser.parseDF()
      if (ddlParser.isUpdate) {
        tsMax = ddl.ts
        sink.mergeData(dmlDS.filter(r => r.ts >= tsMin && r.ts < tsMax))
        sink.updateSchema(ddlParser.df)
        tsMin = tsMax
      }
    })
    sink.mergeData(dmlDS.filter(r => r.ts >= tsMin))
  }

  def filter() = {
    import spark.implicits._
    val table = sink.table
    df.map(r => JacksonUtil.fromJson(r.getString(0), classOf[BinlogRecord]))
      .filter(r => r.ts >= maxTs
        && s"${r.database}.${r.table}".equals(table))
  }
}
