package org.apache.clickhouse.reader

import java.io.IOException
import java.sql.ResultSet

import org.apache.clickhouse.common.{ClickHouseHelper, ClickHouseOptions}
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.sources.v2.reader.InputPartitionReader
import org.apache.spark.sql.types.{DataType, DataTypes, StructField, StructType}
import org.apache.spark.unsafe.types.UTF8String
import ru.yandex.clickhouse.{ClickHouseConnection, ClickHouseStatement}

class ClickHouseInputPartitionReader(schema:StructType, options:ClickHouseOptions) extends InputPartitionReader[InternalRow]{

  private var conn: ClickHouseConnection = _
  private var statment: ClickHouseStatement = _
  private var resultSet: ResultSet = _
  private val helper:ClickHouseHelper = new ClickHouseHelper(options)
  /**
   * Proceed to next record, returns false if there is no more records.
   *
   * If this method fails (by throwing an exception), the corresponding Spark task would fail and
   * get retried until hitting the maximum retry times.
   *
   * @throws IOException if failure happens during disk/network IO like reading files.
   */
  override def next(): Boolean = {
    if(
      (conn == null || conn.isClosed) &&
      (statment == null || statment.isClosed) &&
      (resultSet == null || resultSet.isClosed)
    ){
      conn = helper.getConnection(options)
      statment = conn.createStatement()
      resultSet = statment.executeQuery(s"SELECT * FROM ${options.table}")
    }
    if (resultSet != null && !resultSet.isClosed) {
      resultSet.next()
    } else {
      false
    }
  }

  /**
   * Return the current record. This method should return same value until `next` is called.
   *
   * If this method fails (by throwing an exception), the corresponding Spark task would fail and
   * get retried until hitting the maximum retry times.
   */
  override def get(): InternalRow = {
    // println("======调用get函数，获取当前数据============")
    val fields: Array[StructField] = schema.fields
    //一条数据所有字段的集合
    val record: Array[Any] = new Array[Any](fields.length)

    // 循环取出来所有的列
    for (i <- record.indices) {
      // 每个字段信息
      val field: StructField = fields(i)
      // 列名称
      val fieldName: String = field.name
      // 列数据类型
      val fieldDataType: DataType = field.dataType
      // 根据字段类型，获取对应列的值
      fieldDataType match {
        case DataTypes.BooleanType => record(i) = resultSet.getBoolean(fieldName)
        case DataTypes.DateType => record(i) = DateTimeUtils.fromJavaDate(resultSet.getDate(fieldName))
        case DataTypes.DoubleType => record(i) = resultSet.getDouble(fieldName)
        case DataTypes.FloatType => record(i) = resultSet.getFloat(fieldName)
        case DataTypes.IntegerType => record(i) = resultSet.getInt(fieldName)
        case DataTypes.ShortType => record(i) = resultSet.getShort(fieldName)
        case DataTypes.LongType => record(i) = resultSet.getLong(fieldName)
        case DataTypes.StringType => record(i) = UTF8String.fromString(resultSet.getString(fieldName))
        case DataTypes.TimestampType => record(i) = DateTimeUtils.fromJavaTimestamp(resultSet.getTimestamp(fieldName))
        case DataTypes.ByteType => record(i) = resultSet.getByte(fieldName)
        case DataTypes.NullType => record(i) = StringUtils.EMPTY
        case _ => record(i) = StringUtils.EMPTY
      }
    }

    // 创建InternalRow对象
    new GenericInternalRow(record)
  }

  /**
   * Closes this stream and releases any system resources associated
   * with it. If the stream is already closed then invoking this
   * method has no effect.
   *
   * <p> As noted in {@link AutoCloseable#close()}, cases where the
   * close may fail require careful attention. It is strongly advised
   * to relinquish the underlying resources and to internally
   * <em>mark</em> the {@code Closeable} as closed, prior to throwing
   * the {@code IOException}.
   *
   * @throws IOException if an I/O error occurs
   */
  override def close(): Unit = {
    helper.closeResource(conn,statment,resultSet)
  }
}
