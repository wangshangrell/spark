package org.apache.clickhouse.streamWriter

import org.apache.clickhouse.common.ClickHouseOptions
import org.apache.clickhouse.writer.ClickHouseDataWriterFactory
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.sources.v2.writer.{DataWriterFactory, WriterCommitMessage}
import org.apache.spark.sql.sources.v2.writer.streaming.StreamWriter
import org.apache.spark.sql.types.StructType

class ClickHouseStreamWriter(schema:StructType, option:ClickHouseOptions) extends StreamWriter{

  override def commit(epochId: Long, messages: Array[WriterCommitMessage]): Unit = {}

  override def abort(epochId: Long, messages: Array[WriterCommitMessage]): Unit = {}

  override def createWriterFactory(): DataWriterFactory[InternalRow] = new ClickHouseDataWriterFactory(schema,option)
}
