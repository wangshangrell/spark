package org.apache.clickhouse.writer

import org.apache.clickhouse.common.ClickHouseOptions
import org.apache.spark.sql.sources.v2.DataSourceOptions
import org.apache.spark.sql.sources.v2.writer.{DataSourceWriter, DataWriterFactory, WriterCommitMessage}
import org.apache.spark.sql.types.StructType

class ClickHouseDataSourceWriter(schema:StructType, options:DataSourceOptions) extends DataSourceWriter{

  /**
   * Creates a writer factory which will be serialized and sent to executors.
   *
   * If this method fails (by throwing an exception), the action will fail and no Spark job will be
   * submitted.
   */
  override def createWriterFactory() = new ClickHouseDataWriterFactory(schema,new ClickHouseOptions(options.asMap()))

  override def commit(messages: Array[WriterCommitMessage]): Unit = {}

  override def abort(messages: Array[WriterCommitMessage]): Unit = {}
}
