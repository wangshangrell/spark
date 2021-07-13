package org.apache.clickhouse.reader

import org.apache.clickhouse.common.ClickHouseOptions
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.sources.v2.reader.InputPartition
import org.apache.spark.sql.types.StructType

class ClickHouseInputPartition(schema: StructType, options: ClickHouseOptions) extends InputPartition[InternalRow] {

  /**
   * Returns an input partition reader to do the actual reading work.
   *
   * If this method fails (by throwing an exception), the corresponding Spark task would fail and
   * get retried until hitting the maximum retry times.
   */
  override def createPartitionReader() = new ClickHouseInputPartitionReader(schema, options)
}
