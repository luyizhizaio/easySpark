package com.demo

import org.apache.spark.{SparkConf, SparkContext}

/**
 * tomcat日志统计
 * Created by lichangyue on 2016/8/23.
 */
object LogStat {


  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("logstat").setMaster("local")
    val sc = new SparkContext(conf)


    val log = sc.textFile("hdfs://S7SA053:8020/stat/tomcat.log",3) //分区是为了并行处理
    //过滤
    val filtered = log.filter(_.length >0)
        .filter(line => (line.indexOf("GET")> 0 || line.indexOf("POST")>0))
    //
    val res = filtered.map(line =>{
      if(line.indexOf("GET") >0 ){
        (line.substring(line.indexOf("GET"),line.indexOf("HTTP/1.0")).trim,1)
      } else {
        (line.substring(line.indexOf("POST"),line.indexOf("HTTP/1.0")).trim,1)
      }

    }).reduceByKey(_+_) //sum操作

    //触发action事件执行
    res.collect().foreach( x => println(x._1 +"\t"+ x._2))
  }

}
