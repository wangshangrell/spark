package org.apache.clickhouse.example

import org.apache.spark.sql.SparkSession
import ru.yandex.clickhouse.ClickHouseDriver

object ClickHouseRead {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("clickhouseReader")
      .master("local[2]")
      .config("spark.sql.shuffle.partitions", "2")
      .getOrCreate()

    val ckdf = spark.read.format("clickhouse")
      .option("clickhouse.driver", classOf[ClickHouseDriver].getName)
      .option("clickhouse.url", "jdbc:clickhouse://node2:8123")
      .option("clickhouse.user", "root")
      .option("clickhouse.password", "123456")
      .option("clickhouse.table", "default.sales")
      .load()

    ckdf.show(10, truncate = false)

    spark.close()
  }
}
