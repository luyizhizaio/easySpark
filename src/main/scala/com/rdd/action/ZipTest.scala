package com.rdd.action

import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by lichangyue on 2017/3/24.
 */
object ZipTest {


  def main(args: Array[String]) {
    val conf = new SparkConf().setMaster("local").setAppName(this.getClass().getSimpleName().filter(!_.equals('$')))
    val sc = new SparkContext(conf)


    val rdd1 = sc.makeRDD(1 to 10, 2)

    val rdd2 = sc.makeRDD(1 to 5 ,2)

    val rdd3 = sc.makeRDD(Seq("A","B","C","E","E"),2)

    rdd2.zip(rdd3).collect().foreach(println)
//    (1,A)
//    (2,B)
//    (3,C)
//    (4,E)
//    (5,E)


    val rdd4 = sc.makeRDD(1 to 6 ,2)

    val rdd5 = sc.makeRDD(Seq("A","B","C","D","E") ,2)


    //mapPartitionsWithIndex带着分区的索引
    rdd4.mapPartitionsWithIndex{
      (x,iter) => {
        var result = List[String]()
        while(iter.hasNext){
          result ::=("part_" +x+ "|" +iter.next())
        }
        result.iterator
      }
    }.collect.foreach(println)
//    part_0|3
//    part_0|2
//    part_0|1
//    part_1|6
//    part_1|5
//    part_1|4


    rdd5.mapPartitionsWithIndex{
      (x, iter)=>
        var result = List[String]()
        while(iter.hasNext){
          result ::=("part_" +x+ "|" +iter.next())
        }
        result.iterator
    }.collect.foreach(println)
//    part_0|B
//    part_0|A
//    part_1|E
//    part_1|D
//    part_1|C

    rdd4.zipPartitions(rdd5){
      (rdd4iter, rdd5iter)=>{

        var result = List[String]()
        while(rdd4iter.hasNext && rdd5iter.hasNext){
          result ::=(rdd4iter.next() +"_"+ rdd5iter.next())
        }
        result.iterator
      }
    }.collect().foreach(println)
//    2_B
//    1_A
//    6_E
//    5_D
//    4_C





  }
}
