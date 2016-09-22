package com.rdd.action

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by lichangyue on 2016/9/22.
 */
object ForeachPartitonTest {


  def main(args: Array[String]) {
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR);
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.ERROR);

    val conf = new SparkConf().setMaster("local").setAppName(this.getClass().getSimpleName().filter(!_.equals('$')))
    val sc = new SparkContext(conf)


    var rdd1 = sc.makeRDD(1 to 10,2)

    rdd1.mapPartitionsWithIndex{
      (partIdx,iter) => {
        var part_map = scala.collection.mutable.Map[String,List[Int]]()
        while(iter.hasNext){
          var part_name = "part_" + partIdx;
          var elem = iter.next()
          if(part_map.contains(part_name)) {
            var elems = part_map(part_name)
            elems ::= elem
            part_map(part_name) = elems
          } else {
            part_map(part_name) = List[Int]{elem}
          }
        }
        part_map.iterator

      }
    }.collect.foreach(println(_))
//    (part_0,List(5, 4, 3, 2, 1))
//    (part_1,List(10, 9, 8, 7, 6))

    var allsize = sc.accumulator(0)

    rdd1.foreachPartition { x => {
      allsize += x.size
    }}
    println(allsize.value)
//    10
  }
}
