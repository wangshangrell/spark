package org.apache.clickhouse.common

import java.sql.{ResultSet, ResultSetMetaData}

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types.{DataType, DataTypes, StructField, StructType}
import ru.yandex.clickhouse.{ClickHouseConnection, ClickHouseDataSource, ClickHouseStatement}

import scala.collection.mutable.{ArrayBuffer, ListBuffer}

class ClickHouseHelper(val options: ClickHouseOptions) {

  //get schema information of the table which specified in options
  def getSchema: StructType = {
    //TODO: obtain the schema information of table which was specified in options
    var conn: ClickHouseConnection = null
    var statment: ClickHouseStatement = null
    var resultSet: ResultSet = null

    conn = getConnection(options)
    statment = conn.createStatement()
    resultSet = statment.executeQuery(
      s"""
         |SELECT * FROM ${options.table}
         |""".stripMargin)
    val metatData: ResultSetMetaData = resultSet.getMetaData
    val fields = new Array[StructField](metatData.getColumnCount)
    for (i <- fields.indices) {
      fields(i) = StructField(
        metatData.getColumnName(i + 1),
        ckType2DataType(metatData.getColumnTypeName(i + 1))
      )
    }
    closeResource(conn, statment, resultSet)
    val schema = new StructType(fields)
    schema
  }

  //get ClickHouseConnection instance
  def getConnection(options: ClickHouseOptions): ClickHouseConnection = {
    Class.forName(options.driver)
    val source = new ClickHouseDataSource(options.url)
    source.getConnection(options.user, options.password)
  }

  //close Connection/Statement/ResultSet instance
  def closeResource(conn: ClickHouseConnection, statment: ClickHouseStatement, resultSet: ResultSet): Unit = {
    if (resultSet != null && !resultSet.isClosed) resultSet.close()
    if (statment != null && !statment.isClosed) statment.close()
    if (conn != null && !conn.isClosed) conn.close()
  }

  /**convert ClickHouse DataType to {@link DataTypes}*/
  def ckType2DataType(ckType: String): DataType = {
    ckType match {
      case "IntervalYear" => DataTypes.IntegerType
      case "IntervalQuarter" => DataTypes.IntegerType
      case "IntervalMonth" => DataTypes.IntegerType
      case "IntervalWeek" => DataTypes.IntegerType
      case "IntervalDay" => DataTypes.IntegerType
      case "IntervalHour" => DataTypes.IntegerType
      case "IntervalMinute" => DataTypes.IntegerType
      case "IntervalSecond" => DataTypes.IntegerType
      case "UInt64" => DataTypes.LongType
      case "UInt32" => DataTypes.LongType
      case "UInt16" => DataTypes.IntegerType
      case "UInt8" => DataTypes.IntegerType
      case "Int64" => DataTypes.LongType
      case "Int32" => DataTypes.IntegerType
      case "Int16" => DataTypes.IntegerType
      case "Int8" => DataTypes.IntegerType
      case "Date" => DataTypes.StringType
      case "DateTime" => DataTypes.StringType
      case "Enum8" => DataTypes.StringType
      case "Enum16" => DataTypes.StringType
      case "Float32" => DataTypes.FloatType
      case "Float64" => DataTypes.DoubleType
      case "Decimal32" => DataTypes.createDecimalType()
      case "Decimal64" => DataTypes.createDecimalType()
      case "Decimal128" => DataTypes.createDecimalType()
      case "Decimal" => DataTypes.createDecimalType()
      case "UUID" => DataTypes.StringType
      case "String" => DataTypes.StringType
      case "FixedString" => DataTypes.StringType
      case "Nothing" => DataTypes.NullType
      case "Nested" => DataTypes.StringType
      case "Tuple" => DataTypes.StringType
      case "Array" => DataTypes.StringType
      case "AggregateFunction" => DataTypes.StringType
      case "Unknown" => DataTypes.StringType
      case _ => DataTypes.NullType
    }
  }

  /***/
  def dataType2CKType(dataType: DataType): String = {
    dataType match {
      case DataTypes.ByteType => "Int8"
      case DataTypes.ShortType => "Int16"
      case DataTypes.IntegerType => "Int32"
      case DataTypes.FloatType => "Float32"
      case DataTypes.DoubleType => "Float64"
      case DataTypes.LongType => "Int64"
      case DataTypes.DateType => "DateTime"
      case DataTypes.TimestampType => "DateTime"
      case DataTypes.StringType => "String"
      case DataTypes.NullType => "String"
    }
  }

  def createSQLFromRecord(record: InternalRow, schema: StructType): String = {
    options.operation.toLowerCase match {
      case "insert" => createInsertSQL(record, schema)
      case "update" => createUpdateSQL(record, schema)
      case "delete" => createDeleteSQL(record, schema)
      case _ => throw new RuntimeException(s"illegal operation type ${options.operation}, {'insert' 'update' 'delete'}")
    }
  }

  def createInsertSQL(record: InternalRow, schema: StructType): String = {
    val fields = schema.fields
    val typeList: ListBuffer[String] = new ListBuffer[String]
    val valueList: ListBuffer[String] = new ListBuffer[String]
    for (i <- fields.indices) {
      val typeStr = dataType2CKType(fields(i).dataType)
      val value = fields(i).dataType match {
        case DataTypes.ByteType => record.get(i, fields(i).dataType).toString
        case DataTypes.ShortType => record.get(i, fields(i).dataType).toString
        case DataTypes.IntegerType => record.get(i, fields(i).dataType).toString
        case DataTypes.FloatType => record.get(i, fields(i).dataType).toString
        case DataTypes.DoubleType => record.get(i, fields(i).dataType).toString
        case DataTypes.LongType => record.get(i, fields(i).dataType).toString
        case _ => s"'${record.get(i, fields(i).dataType).toString}'"
      }
      typeList.append(typeStr)
      valueList.append(value)
    }
    val sql = s"INSERT INTO ${options.table}(${typeList.mkString(", ")}) VALUES(${valueList.mkString(",")})"
    sql
  }

  def createUpdateSQL(record: InternalRow, schema: StructType): String = {
    val fields = schema.fields
    val kvList: ListBuffer[String] = new ListBuffer[String]
    val pkvList: ListBuffer[String] = new ListBuffer[String]
    val primaryKeys = options.getPrimaryKey.split(",")
    if (primaryKeys.length < 1) {
      throw new RuntimeException("at least one column should be specified as primary key during update operation")
    }
    for (i <- fields.indices) {
      val name = fields(i).name
      val value = fields(i).dataType match {
        case DataTypes.ByteType => record.get(i, fields(i).dataType).toString
        case DataTypes.ShortType => record.get(i, fields(i).dataType).toString
        case DataTypes.IntegerType => record.get(i, fields(i).dataType).toString
        case DataTypes.FloatType => record.get(i, fields(i).dataType).toString
        case DataTypes.DoubleType => record.get(i, fields(i).dataType).toString
        case DataTypes.LongType => record.get(i, fields(i).dataType).toString
        case _ => s"'${record.get(i, fields(i).dataType).toString}'"
      }
      if (primaryKeys.contains[String](name)) {
        pkvList.append(s"${name} = ${value}")
      } else {
        kvList.append(s"${name} = ${value}")
      }
    }
    val sql = s"ALTER TABLE ${options.table} UPDATE ${kvList.mkString(",")} WHERE ${pkvList.mkString(" and ")}"
    sql
  }

  def createDeleteSQL(record: InternalRow, schema: StructType): String = {
    val fields = schema.fields
    val pkvList: ListBuffer[String] = new ListBuffer[String]
    val primaryKeys = options.getPrimaryKey.split(",")
    if (primaryKeys.length < 1) {
      throw new RuntimeException("at least one column should be specified as primary key during delete operation")
    }
    for (i <- fields.indices) {
      val name = fields(i).name
      val value = fields(i).dataType match {
        case DataTypes.ByteType => record.get(i, fields(i).dataType).toString
        case DataTypes.ShortType => record.get(i, fields(i).dataType).toString
        case DataTypes.IntegerType => record.get(i, fields(i).dataType).toString
        case DataTypes.FloatType => record.get(i, fields(i).dataType).toString
        case DataTypes.DoubleType => record.get(i, fields(i).dataType).toString
        case DataTypes.LongType => record.get(i, fields(i).dataType).toString
        case _ => s"'${record.get(i, fields(i).dataType).toString}'"
      }
      if (primaryKeys.contains[String](name)) {
        pkvList.append(s"${name} = ${value}")
      }
    }
    val sql = s"ALTER TABLE ${options.table} DELETE WHERE ${pkvList.mkString(" and ")}"
    sql
  }

  def getBatchSQL(sqlBuffer: ArrayBuffer[String]): ArrayBuffer[String] = {
    options.operation.toLowerCase match {

      case "insert" =>
        val batchSQL: String = sqlBuffer.map(sql => {
          val i = sql.indexOf("VALUES") + 6
          sql.substring(i)
        }).mkString(", ")
        ArrayBuffer(s"INSERT INTO ${options.table} VALUES${batchSQL}")

      case "update" => sqlBuffer

      case "delete" =>
        val batchSQL = sqlBuffer.map { sql => {
          val i = sql.indexOf("WHERE") + 5
          s"(${sql.substring(i)})"
        }
        }.mkString(" or ")
        ArrayBuffer(s"ALTER TABLE ${options.table} DELETE WHERE ${batchSQL}")

      case _ => throw new RuntimeException(s"illegal operation type ${options.operation}, {'insert' 'update' 'delete'}")
    }
  }

  def createTable(schema: StructType, conn: ClickHouseConnection, statement: ClickHouseStatement): Unit = {
    //TODO: create table
    val fields = schema.fields
    val fieldBuffer = new Array[String](fields.length)
    for (i <- fields.indices) {
      val fieldName: String = fields(i).name
      val fieldType: String = dataType2CKType(fields(i).dataType)
      fieldBuffer(i) = s"${fieldName} ${fieldType}"
    }
    val createTableSQL = s"""CREATE TABLE IF NOT EXISTS ${options.table}
                           |(
                           |${fieldBuffer.mkString(",\n")}
                           |)
                           |ENGINE = MergeTree
                           |PRIMARY KEY (${options.getPrimaryKey})
                           |ORDER BY (${options.getPrimaryKey})
                           |""".stripMargin

    statement.executeUpdate(createTableSQL)
  }

}
