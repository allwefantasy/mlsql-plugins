package tech.mlsql.plugins.udf

import org.apache.spark.sql.UDFRegistration
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat

/**
 * 8/4/2020 WilliamZhu(allwefantasy@gmail.com)
 */
object DateUDF {
  def parseDateAsLong(uDFRegistration: UDFRegistration): Unit = {
    uDFRegistration.register("parse_date", (date: String, pattern: String) => {
      DateTime.parse(date, DateTimeFormat.forPattern(pattern)).getMillis
    })
  }

  def parseLongAsDate(uDFRegistration: UDFRegistration): Unit = {
    uDFRegistration.register("parse_date", (date: Long, pattern: String) => {
      val dt = new DateTime().withMillis(date)
      dt.toString(pattern)
    })
  }
}
