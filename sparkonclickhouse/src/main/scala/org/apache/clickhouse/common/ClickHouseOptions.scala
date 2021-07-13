package org.apache.clickhouse.common

import java.util

/**
 * 解析自定义ClickHouse数据源时，接收到的客户端封装的参数options，将参数的map集合进行解析
 */
class ClickHouseOptions(var options: util.Map[String, String]) extends Serializable {

  // 定义ClickHouse的驱动类名称
  val CLICKHOUSE_DRIVER_KEY = "clickhouse.driver"
  // 定义ClickHouse的连接地址
  val CLICKHOUSE_URL_KEY = "clickhouse.url"
  // 定义ClickHouse的用户名
  val CLICKHOUSE_USER_KEY = "clickhouse.user"
  // 定义ClickHouse的密码
  val CLICKHOUSE_PASSWORD_KEY = "clickhouse.password"
  //定义ClickHouse写入的操作类型
  val OPERATION_TYPE_KEY = "clickhouse.operation.type"

  // 定义ClickHouse的表名
  val CLICKHOUSE_TABLE_KEY = "clickhouse.table"
  // 定义ClickHouse的表不存在是否可以自动创建表
  val CLICKHOUSE_AUTO_CREATE_KEY = "clickhouse.auto.create"

  // 定义ClickHouse表的主键列
  val CLICKHOUSE_PRIMARY_KEY = "clickhouse.primary.key"
  val CLICKHOUSE_OPERATE_FIELD_KEY = "clickhouse.operate.field"

  // 根据key在map对象中获取指定key的value值，并且将value转换成指定的数据类型返回
  def getValue[T](key: String): T = {
    // 判断Key是否存在，如果存在获取值，不存在返回null
    val value = if (options.containsKey(key)) options.get(key) else null
    // 类型转换
    value.asInstanceOf[T]
  }

  // 根据driver的key获取driver的value
  def driver: String = getValue[String](CLICKHOUSE_DRIVER_KEY)

  // 根据url的key获取url的value
  def url: String = getValue[String](CLICKHOUSE_URL_KEY)

  // 根据user的key获取user的value
  def user: String = getValue[String](CLICKHOUSE_USER_KEY)

  // 根据user的key获取password的value
  def password: String = getValue[String](CLICKHOUSE_PASSWORD_KEY)

  // 根据table的key获取table的value
  def table: String = getValue[String](CLICKHOUSE_TABLE_KEY)


  def operation: String = getValue[String](OPERATION_TYPE_KEY)

  // 获取是否自动创建表
  def autoCreateTable: Boolean = {
    // 先从Map集合获取是否创建表，默认值为false（不创建）
    val autoValue: String = options.getOrDefault(CLICKHOUSE_AUTO_CREATE_KEY, "false")
    // 将字符串true或false转换为Boolean类型
    autoValue.toLowerCase() match {
      case "true" => true
      case "false" => false
    }
  }

  // 获取主键列，默认主键名称为：id
  def getPrimaryKey: String = {
    options.getOrDefault(CLICKHOUSE_PRIMARY_KEY, "id")
  }

  // 获取数据的操作类型字段名称
  def getOperateField: String = {
    options.getOrDefault(CLICKHOUSE_OPERATE_FIELD_KEY, "opType")
  }
}
