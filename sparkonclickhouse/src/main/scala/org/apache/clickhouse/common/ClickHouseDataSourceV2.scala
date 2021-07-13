package org.apache.clickhouse.common

import java.util.Optional

import org.apache.clickhouse.reader.ClickHouseDataSourceReader
import org.apache.clickhouse.streamWriter.ClickHouseStreamWriter
import org.apache.clickhouse.writer.ClickHouseDataSourceWriter
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.sources.DataSourceRegister
import org.apache.spark.sql.sources.v2._
import org.apache.spark.sql.sources.v2.writer.DataSourceWriter
import org.apache.spark.sql.sources.v2.writer.streaming.StreamWriter
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.types.StructType

class ClickHouseDataSourceV2 extends DataSourceV2
  with ReadSupport with WriteSupport with StreamWriteSupport
  with DataSourceRegister {

  /**
   * Creates a {@link DataSourceReader} to scan the data from this data source.
   *
   * If this method fails (by throwing an exception), the action will fail and no Spark job will be
   * submitted.
   *
   * @param options the options for the returned data source reader, which is an immutable
   *                case-insensitive string-to-string map.
   *                By default this method throws { @link UnsupportedOperationException}, implementations should
   *                override this method to handle user specified schema.
   */
  override def createReader(options: DataSourceOptions) = new ClickHouseDataSourceReader(options)

  /**
   * Creates an optional {@link DataSourceWriter} to save the data to this data source. Data
   * sources can return None if there is no writing needed to be done according to the save mode.
   *
   * If this method fails (by throwing an exception), the action will fail and no Spark job will be
   * submitted.
   *
   * @param writeUUID A unique string for the writing job. It's possible that there are many writing
   *                  jobs running at the same time, and the returned { @link DataSourceWriter} can
   *                                                                          use this job id to distinguish itself from other jobs.
   * @param schema the schema of the data to be written.
   * @param mode   the save mode which determines what to do when the data are already in this data
   *               source, please refer to { @link SaveMode} for more details.
   * @param options the options for the returned data source writer, which is an immutable
   *                case-insensitive string-to-string map.
   * @return a writer to append data to this data source
   */
  override def createWriter(writeUUID: String, schema: StructType, mode: SaveMode, options: DataSourceOptions): Optional[DataSourceWriter] = mode match {
    case SaveMode.Append =>
      val writer = new ClickHouseDataSourceWriter(schema, options)
      Optional.of[DataSourceWriter](writer)
    case _ => Optional.empty[DataSourceWriter]()
  }

  /**
   * Creates an optional {@link StreamWriter} to save the data to this data source. Data
   * sources can return None if there is no writing needed to be done.
   *
   * @param queryId A unique string for the writing query. It's possible that there are many
   *                writing queries running at the same time, and the returned
   *                { @link DataSourceWriter} can use this id to distinguish itself from others.
   * @param schema the schema of the data to be written.
   * @param mode   the output mode which determines what successive epoch output means to this
   *               sink, please refer to { @link OutputMode} for more details.
   * @param options the options for the returned data source writer, which is an immutable
   *                case-insensitive string-to-string map.
   */
  override def createStreamWriter(queryId: String, schema: StructType, mode: OutputMode, options: DataSourceOptions): StreamWriter = {
    new ClickHouseStreamWriter(schema,new ClickHouseOptions(options.asMap()))
  }

  override def shortName(): String = "clickhouse"
}
