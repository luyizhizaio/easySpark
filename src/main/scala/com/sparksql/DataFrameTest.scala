package com.sparksql

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by lichangyue on 2016/8/26.
 */
object DataFrameTest {

  def main(args: Array[String]) {

    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR);
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.ERROR);

    val conf = new SparkConf().setAppName("dataFrametest").setMaster("local")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    val df = sqlContext.read.json("hdfs://S7SA053:8020/stat/person.json")
    //查看df中的数据
    df.show();
    //查看schema
    df.printSchema()
    //查看某个字段的值
    df.select("name").show()

    df.select(df.col("name"),df.col("age").plus(1)).show()

    df.filter(df.col("age").gt(25)).show()

    df.groupBy("age").count().show()

    df.select(df.col("id"),df.col("name"),df.col("age")).foreach(x => {

      println("col1:"+x.get(0)+",col2:"+x.get(1) +",col3:"+x.get(2))
    })

    //foreachPartition
    df.select(df.col("id"),df.col("name"),df.col("age")).foreachPartition(iterator => {
      iterator.foreach(x => {
        println("id:"+x.getAs("id") +",name:"+x.getAs("name") +",age:"+x.getAs("age"))
      })


    })



  }

}
