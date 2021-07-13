package org.apache.clickhouse.reader

import java.util

import org.apache.clickhouse.common.{ClickHouseHelper, ClickHouseOptions}
import org.apache.spark.sql.sources.v2.DataSourceOptions
import org.apache.spark.sql.sources.v2.reader.DataSourceReader
import org.apache.spark.sql.types.StructType

class ClickHouseDataSourceReader(options: DataSourceOptions) extends DataSourceReader {

  val ckOptions = new ClickHouseOptions(options.asMap())

  /**
   * Returns the actual schema of this data source reader, which may be different from the physical
   * schema of the underlying storage, as column pruning or other optimizations may happen.
   *
   * If this method fails (by throwing an exception), the action will fail and no Spark job will be
   * submitted.
   */
  override def readSchema(): StructType = {
    val helper = new ClickHouseHelper(ckOptions)
    helper.getSchema
  }

  /**
   * Returns a list of {@link InputPartition}s. {@link ClickHouseInputPartition} Each {@link InputPartition} is responsible for
   * creating a data reader to output data of one RDD partition. The number of input partitions
   * returned here is the same as the number of RDD partitions this scan outputs.
   *
   * Note that, this may not be a full scan if the data source reader mixes in other optimization
   * interfaces like column pruning, filter push-down, etc. These optimizations are applied before
   * Spark issues the scan request.
   *
   * If this method fails (by throwing an exception), the action will fail and no Spark job will be
   * submitted.
   */
  override def planInputPartitions() = {
    util.Arrays.asList(new ClickHouseInputPartition(readSchema(), ckOptions))
  }
}
