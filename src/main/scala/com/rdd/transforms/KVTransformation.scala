package com.rdd.transforms

import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by lichangyue on 2016/9/21.
 */
object KVTransformation {


  def main(args: Array[String]) {
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR);
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.ERROR);

    val conf = new SparkConf().setMaster("local").setAppName(this.getClass().getSimpleName().filter(!_.equals('$')))
    val sc = new SparkContext(conf)


    val rdd1:RDD[(String,Int)] = sc.makeRDD(Array(("A",1),("A",2),("B",1),("B",2),("C",1)),4)

    rdd1.mapPartitionsWithIndex{
      (partIdx,iter) => {
        var part_map = scala.collection.mutable.Map[String,List[(String,Int)]]()
        while(iter.hasNext){
          var part_name = "part_" + partIdx;
          var elem = iter.next()
          if(part_map.contains(part_name)) {
            var elems = part_map(part_name)
            elems ::= elem
            part_map(part_name) = elems
          } else {
            part_map(part_name) = List[(String,Int)]{elem}
          }
        }
        part_map.iterator

      }
    }.collect.foreach(println(_))
//    (part_0,List((A,1)))
//    (part_1,List((A,2)))
//    (part_2,List((B,1)))
//    (part_3,List((C,1), (B,2)))

    rdd1.combineByKey(
      (v : Int) => v + "_",
      (c : String, v : Int) => c + "@" + v ,//分区内部执行合并
      (c1 : String, c2 : String) =>  c1 + "$" + c2 //分区之间执行的合并
     ).collect.foreach(println)
//    (A,1_$2_)
//    (B,1_$2_)
//    (C,1_)


    //~~~~~~~~~~~~~~~~~~
    //foldByKey
    //~~~~~~~~~~~~~~~~~~~~~~~~~

    println("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~")
    var rdd2 = sc.makeRDD(Array(("A",0),("A",2),("B",1),("B",2),("C",1)) ,4)
    rdd2.foldByKey(0)(_+_).collect.foreach(println(_))
//    (A,2)
//    (B,3)
//    (C,1)
    //~~~~~~~~~~~~~~~~~~
    //groupByKey
    //~~~~~~~~~~~~~~~~~~~~~~~~~
    var rdd3 = sc.makeRDD(Array(("A",0),("A",2),("B",1),("B",2),("C",1)))

    rdd3.groupByKey().collect.foreach(println)
//    (B,CompactBuffer(1, 2))
//    (A,CompactBuffer(0, 2))
//    (C,CompactBuffer(1))





    println("\n\nreduceByKey~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~")



    var rdd4 = sc.makeRDD(Array(("A",0),("A",2),("B",1),("B",2),("C",1)),10)

    println(rdd4.partitions.size)

    var rdd5 = rdd4.reduceByKey((x,y) => x + y)

    rdd5.collect.foreach(println)


    println(rdd5.partitions.size)

    var rdd6 = rdd4.reduceByKey(new org.apache.spark.HashPartitioner(2),(x,y) => x + y)

    rdd6.collect.foreach(println)

    println(rdd6.partitions.size)

//    10
//    (A,2)
//    (B,3)
//    (C,1)
//    10
//    (B,3)
//    (A,2)
//    (C,1)
//    2


    var rdd11 = sc.makeRDD(Array(("A",0),("A",2),("B",1),("B",2),("C",1)))
//    rdd1: org.apache.spark.rdd.RDD[(String, Int)] = ParallelCollectionRDD[91] at makeRDD at :21

    rdd11.reduceByKeyLocally((x,y) => x + y).foreach(m =>println( m._1 + "->" + m._2))
//    res90: scala.collection.Map[String,Int] = Map(B -> 3, A -> 2, C -> 1)
//    A->2
//    B->3
//    C->1








  }


}
