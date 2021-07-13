package org.apache.clickhouse.writer

import java.io.IOException

import org.apache.clickhouse.common.{ClickHouseHelper, ClickHouseOptions}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.sources.v2.writer.{DataWriter, WriterCommitMessage}
import org.apache.spark.sql.types.StructType

import scala.collection.mutable.ArrayBuffer

class ClickHouseDataWriter(partitionId: Int, taskId: Long, epochId: Long, schema: StructType, options: ClickHouseOptions) extends DataWriter[InternalRow] {

  var sqlBuffer: ArrayBuffer[String] = new ArrayBuffer[String]
  var msgBuffer: ArrayBuffer[String] = new  ArrayBuffer[String]
  val helper: ClickHouseHelper = new ClickHouseHelper(options)
  var counter = 0

  /**
   * Writes one record.
   *
   * If this method fails (by throwing an exception), {@link #abort()} will be called and this
   * data writer is considered to have been failed.
   *
   * @throws IOException if failure happens during disk/network IO like writing files.
   */
  override def write(record: InternalRow): Unit = {
    val sql = helper.createSQLFromRecord(record, schema)
    sqlBuffer.append(sql)
    msgBuffer.append(s"task = ${taskId}, partition = ${partitionId}, epoch = ${epochId}, record${counter} has created sql successful")
  }

  /**
   * Commits this writer after all records are written successfully, returns a commit message which
   * will be sent back to driver side and passed to
   * {@link DataSourceWriter#commit(WriterCommitMessage[])}.
   *
   * The written data should only be visible to data source readers after
   * {@link DataSourceWriter#commit(WriterCommitMessage[])} succeeds, which means this method
   * should still "hide" the written data and ask the {@link DataSourceWriter} at driver side to
   * do the final commit via {@link WriterCommitMessage}.
   *
   * If this method fails (by throwing an exception), {@link #abort()} will be called and this
   * data writer is considered to have been failed.
   *
   * @throws IOException if failure happens during disk/network IO like writing files.
   */
  override def commit(): WriterCommitMessage = {

    val conn = helper.getConnection(options)
    val statement = conn.createStatement()
    val batchSQLs = helper.getBatchSQL(sqlBuffer)

    if(options.autoCreateTable){
      helper.createTable(schema,conn,statement)
    }

    batchSQLs.foreach(batchSQL => {
      print(s"executing batchSQL: ${batchSQL} .......\n")
      statement.executeUpdate(batchSQL)
    })

    helper.closeResource(conn,statement,null)
    sqlBuffer.clear()

    new WriterCommitMessage {
      msgBuffer
    }
  }

  override def abort(): Unit = {}
}
