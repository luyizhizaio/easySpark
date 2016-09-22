package com.rdd.action

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

/** aggregate用户聚合RDD中的元素，先使用seqOp将RDD中每个分区中的T类型元素聚合成U类型，
  * 再使用combOp将之前每个分区聚合后的U类型聚合成U类型，
  * 特别注意seqOp和combOp都会使用zeroValue的值，zeroValue的类型为U。
  * Created by lichangyue on 2016/9/22.
 */
object AggreateTest {

  def main(args: Array[String]) {
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR);
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.ERROR);

    val sparkConf = new SparkConf().setAppName("FirstSpark").setMaster("local")
    //创建上下文 参数（spark集群地址，spark程序标示，spark安装路径，需要传入这个程序的jar包路径）
    val sc = new SparkContext(sparkConf)

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

    val result = rdd1.aggregate(1)(
    {(x:Int,y:Int) => x+y}, //seqOp 分区内操作
    {(a:Int ,b:Int) => a+b}//combOp 分区间合并
    )
    println(result)
//    58



    val result2 = rdd1.fold(1)(
      (x,y) => x +y
    )

    println(result2)
    //    58










  }

}
