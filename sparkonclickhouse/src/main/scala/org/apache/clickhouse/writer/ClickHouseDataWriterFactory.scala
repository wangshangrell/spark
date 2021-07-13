package org.apache.clickhouse.writer

import org.apache.clickhouse.common.ClickHouseOptions
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.sources.v2.writer.DataWriterFactory
import org.apache.spark.sql.types.StructType

class ClickHouseDataWriterFactory(schema: StructType, options: ClickHouseOptions) extends DataWriterFactory[InternalRow] {

  /**
   * Returns a data writer to do the actual writing work. Note that, Spark will reuse the same data
   * object instance when sending data to the data writer, for better performance. Data writers
   * are responsible for defensive copies if necessary, e.g. copy the data before buffer it in a
   * list.
   *
   * If this method fails (by throwing an exception), the action will fail and no Spark job will be
   * submitted.
   *
   * @param partitionId A unique id of the RDD partition that the returned writer will process.
   *                    Usually Spark processes many RDD partitions at the same time,
   *                    implementations should use the partition id to distinguish writers for
   *                    different partitions.
   * @param taskId      A unique identifier for a task that is performing the write of the partition
   *               data. Spark may run multiple tasks for the same partition (due to speculation
   *                    or task failures, for example).
   * @param epochId     A monotonically increasing id for streaming queries that are split in to
   *                    discrete periods of execution. For non-streaming queries,
   *                    this ID will always be 0.
   */
  override def createDataWriter(partitionId: Int, taskId: Long, epochId: Long) = new ClickHouseDataWriter(partitionId, taskId, epochId, schema, options)
}
