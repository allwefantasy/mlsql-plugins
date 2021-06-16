package tech.mlsql.plugins.canal.mysql.statement

import com.alibaba.druid.sql.ast.expr.SQLValuableExpr
import com.alibaba.druid.sql.ast.statement.{SQLAlterTableAddColumn, SQLAlterTableDropColumnItem, SQLAlterTableStatement, SQLTruncateStatement}
import com.alibaba.druid.sql.dialect.mysql.ast.statement.{MySqlAlterTableChangeColumn, MySqlAlterTableModifyColumn}
import com.alibaba.druid.sql.dialect.mysql.parser.MySqlStatementParser
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, typedLit}
import tech.mlsql.plugins.canal.mysql.JdbcTypeParser

import scala.collection.JavaConverters._

/**
  * Created by zhuml on 2021/6/10.
  */
class DDLStatementParser(var df: DataFrame, sql: String) extends MySqlStatementParser(sql: String) {

  var isUpdate = false

  def parseDF() = {
    val stmt = parseStatement()
    stmt match {
      case alter: SQLAlterTableStatement =>
        alter.getItems.asScala.foreach(iterm =>
          iterm match {
            case add: SQLAlterTableAddColumn =>
              alterAddColumn(add)
            case drop: SQLAlterTableDropColumnItem =>
              alterDropColumn(drop)
            case change: MySqlAlterTableChangeColumn =>
              alterChangeColumn(change)
            case modify: MySqlAlterTableModifyColumn =>
              alterModifyColumn(modify)
          })
      case _: SQLTruncateStatement =>
        truncateTable()
    }
  }

  def truncateTable() = {
    df = df.limit(0)
    isUpdate = true
  }

  def alterAddColumn(add: SQLAlterTableAddColumn) = {
    add.getColumns.asScala.foreach(column => {
      val newType = JdbcTypeParser.sqlTypeToDataType(column.getDataType)
      val _col = {
        column.getDefaultExpr match {
          case expr: SQLValuableExpr => typedLit(expr.getValue)
          case _ => typedLit[String](null)
        }
      }.cast(newType.simpleString)
      df = df.withColumn(column.getNameAsString, _col)
      isUpdate = true
    })
  }

  def alterDropColumn(drop: SQLAlterTableDropColumnItem) = {
    drop.getColumns.asScala.foreach(column => {
      df = df.drop(column.getSimpleName)
      isUpdate = true
    })
  }

  def alterChangeColumn(change: MySqlAlterTableChangeColumn) = {
    val oldName = change.getColumnName.getSimpleName
    val newName = change.getNewColumnDefinition.getNameAsString
    val oldTypeString = df.dtypes.toMap.get(oldName).get
    val newType = JdbcTypeParser.sqlTypeToDataType(change.getNewColumnDefinition.getDataType)
    if (!oldTypeString.equals(newType.simpleString)) {
      df = df.withColumn(newName, col(oldName).cast(newType))
      isUpdate = true
      if (!oldName.equals(newName)) {
        df = df.drop(oldName)
      }
    } else if (!oldName.equals(newName)) {
      df = df.withColumnRenamed(oldName, newName)
      isUpdate = true
    }
  }

  def alterModifyColumn(modify: MySqlAlterTableModifyColumn) = {
    val name = modify.getNewColumnDefinition.getNameAsString
    val oldTypeString = df.dtypes.toMap.get(name).get
    val newType = JdbcTypeParser.sqlTypeToDataType(modify.getNewColumnDefinition.getDataType)
    if (!newType.simpleString.equals(oldTypeString)) {
      df = df.withColumn(name, col(name).cast(oldTypeString))
      isUpdate = true
    }
  }

}

