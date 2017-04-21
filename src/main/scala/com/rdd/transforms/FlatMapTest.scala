package com.rdd.transforms

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by lichangyue on 2016/9/28.
 */
object FlatMapTest {


  def main(args: Array[String]) {
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR);
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.ERROR);
    val conf = new SparkConf().setMaster("local").setAppName(this.getClass().getSimpleName().filter(!_.equals('$')))
    val sc = new SparkContext(conf)


   /* val text = sc.textFile("file/temp/text.txt" ,3)

    text.mapPartitionsWithIndex{
      (partIdx,iter) => {
        var part_map = scala.collection.mutable.Map[String,List[String]]()
        while(iter.hasNext){
          var part_name = "part_" + partIdx;
          var elem = iter.next()
          if(part_map.contains(part_name)) {
            var elems = part_map(part_name)
            elems ::= elem
            part_map(part_name) = elems
          } else {
            part_map(part_name) = List[String]{elem}
          }
        }
        part_map.iterator

      }
    }.collect.foreach(println)


    val flatmapResult = text.flatMap(line => line.split("\\s+"))



    flatmapResult.mapPartitionsWithIndex{
      (partIdx,iter) => {
        var part_map = scala.collection.mutable.Map[String,List[String]]()
        while(iter.hasNext){
          var part_name = "part_" + partIdx;
          var elem = iter.next()
          if(part_map.contains(part_name)) {
            var elems = part_map(part_name)
            elems ::= elem
            part_map(part_name) = elems
          } else {
            part_map(part_name) = List[String]{elem}
          }
        }
        part_map.iterator

      }
    }.collect.foreach(println)




    flatmapResult.collect.foreach(println(_))
*/

    val arr=sc.parallelize(Array(("AD",1),("BE",2),("CF",3)))

    arr.flatMap(x=>{
      x._1+x._2
    }).foreach(println)
//    A
//    D
//    1
//    B
//    E
//    2
//    C
//    F
//    3





    val arr2=sc.parallelize(Array(("AD",1),("BE",2),("CF",3)))
    arr2.map(x=>(x._1+x._2)).foreach(println)
//    AD1
//    BE2
//    CF3

  }

}
