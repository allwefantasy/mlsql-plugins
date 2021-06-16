package tech.mlsql.plugins.canal.mysql

import com.alibaba.druid.sql.ast.{SQLDataType, SQLDataTypeImpl}
import org.apache.spark.sql.types.{DataType, DecimalType, StructField, StructType}

/**
  * Created by zhuml on 2021/6/11.
  */
object JdbcTypeParser {

  val UNSIGNED = """.*(unsigned)""".r

  // 判断是否为有符号数
  def isSigned(typeName: String) = {
    typeName.trim match {
      case UNSIGNED(unsigned) => false
      case _ => true
    }
  }

  val FIXED_DECIMAL = """decimal\(\s*(\d+)\s*,\s*(\-?\d+)\s*\)""".r
  val FIXED_NUMERIC = """numeric\(\s*(\d+)\s*,\s*(\-?\d+)\s*\)""".r
  val FIXED_SCALE = """\w*\(\s*(\d+)\s*\)""".r


  // decimal/numeric 数据类型 具有precision固定精度（最大位数）和scale小数位数（点右侧的位数）的十进制数。
  def parsePrecisionScale(name: String) = {
    name match {
      case "decimal" | "numeric" => Array(DecimalType.SYSTEM_DEFAULT.precision, DecimalType.SYSTEM_DEFAULT.scale)
      case FIXED_DECIMAL(precision, scale) => Array(precision.toInt, scale.toInt)
      case FIXED_NUMERIC(precision, scale) => Array(precision.toInt, scale.toInt)
      case FIXED_SCALE(scale) => Array(scale.toInt, 0)
      case _ => Array(0, 0)
    }
  }

  def getMysqlStructType(sqlTypeMap: Map[String, Int], mysqlTypeMap: Map[String, String]): StructType = {

    val fields = mysqlTypeMap.map(k => {
      val sqlType = sqlTypeMap(k._1)
      val Array(precision, scale) = parsePrecisionScale(k._2)
      val signed = isSigned(k._2)
      val columnType = getCatalystTypePrivate(sqlType, precision, scale, signed).asInstanceOf[DataType]
      StructField(k._1, columnType)
    }).toArray
    new StructType(fields)
  }

  def getSqlTypeCode(name: String): Integer = {
    val _type = """\w*""".r.findFirstIn(name).getOrElse("").toUpperCase
    MysqlType.valueOf(_type).getVendorTypeNumber
  }

  def sqlTypeToDataType(sqlDataType: SQLDataType): DataType = {
    val name = sqlDataType.getName
    val Array(precision, scale) = parsePrecisionScale(name)
    val sqlType = getSqlTypeCode(name)
    getCatalystTypePrivate(sqlType, precision, scale, !sqlDataType.asInstanceOf[SQLDataTypeImpl].isUnsigned).asInstanceOf[DataType]
  }

  // JDBC type to Catalyst type
  lazy val getCatalystTypePrivate = {
    import scala.reflect.runtime.{universe => ru}
    val classMirror = ru.runtimeMirror(getClass.getClassLoader)
    val JdbcUtils = classMirror.staticModule("org.apache.spark.sql.execution.datasources.jdbc.JdbcUtils")
    val methods = classMirror.reflectModule(JdbcUtils)
    val instanceMirror = classMirror.reflect(methods.instance)
    val method = methods.symbol.typeSignature.member(ru.TermName("getCatalystType")).asMethod

    instanceMirror.reflectMethod(method)
  }


}
