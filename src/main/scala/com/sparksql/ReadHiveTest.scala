package com.sparksql

import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by lichangyue on 2016/10/25.
 * 读取hive表中的数据 stat.weixin_count
 */
object ReadHiveTest {

  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("ReadHiveTest")
    val sc = new SparkContext(conf)
    val hiveContext =new HiveContext(sc)

    //隐式转换rdd成df
    import hiveContext.implicits._
    import hiveContext.sql

    //1.读取hive表
    sql("USE stat")
    val df = sql("select * from weixin_count")

    val count =df.count()
    println(s"COUNT: $count")

    df.take(10).foreach(println(_))


    //2.写入hive表
    //创建表weixin2
    sql("USE stat")
    sql("create table if not exists weixin2( ufpd string, num INT)")
    //将weixin.count表中的数据插入到表2中
    sql("insert overwrite table weixin2 select fp, cn from weixin_count")

    sc.stop()

  }
}
