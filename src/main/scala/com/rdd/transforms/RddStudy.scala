package com.rdd.transforms

import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by lichangyue on 2016/8/19.
 */
object RddStudy {


def main(args:Array[String]): Unit ={


 /* val conf = new SparkConf().setAppName("rddstudy").setMaster("local")
  val sc = new SparkContext(conf)*/

  val sparkConf = new SparkConf().setAppName("FirstSpark").setMaster("local")
  //创建上下文 参数（spark集群地址，spark程序标示，spark安装路径，需要传入这个程序的jar包路径）
  val sc = new SparkContext(sparkConf)

  /** 1.hdfs*/
  //从hdfs上读取文件
 /* val file =sc.textFile("hdfs://S1SA300:8020/stat/000000_0")
  //对file进行过滤，判断file的每一行字符串中是否包含hello World , 生成filterRDD
  val filterRDD = file.filter(_.contains("1"))
  //缓存filterRDD 以便重用
  filterRDD.cache()
  //计数操作：返回包含字符串的文本行数
  val count = filterRDD.count()
  println(count)*/

  //2.依赖关系
 /* val rdd= sc.makeRDD(1 to 10)
  val  maprdd = rdd.map(x => (x,x))
  println(maprdd.dependencies)
  val shufflerdd = maprdd.partitionBy(new HashPartitioner(3))
  println(shufflerdd.dependencies)*/

  //3.rdd 分区计算compute

 /* val rdd = sc.parallelize(1 to 100 ,2)
  val map_rdd = rdd.map(a => a+1)

  val filter_rdd = map_rdd.filter(a=>(a>3))

  val context =new TaskContext(0,0,0)

  val iter0  = filter_rdd.compute(filter_rdd.partitions(0),context)

  val  list = iter0.toList
  println(list)*/

  //4.分区函数
 /* val rdd = sc.makeRDD(1 to 10 ,2).map(x =>(x,x))
  val first = rdd.first()
  println(first)
  val par= rdd.partitioner
  println(par)

  val group_rdd =rdd.groupByKey(new HashPartitioner(3))
  println(group_rdd.partitioner)
  println(group_rdd.collect())*/

  //5.创建操作
 /* val rdd = sc.makeRDD(1 to 10 ,3)
  val collect = Seq((1 to 10 ,Seq("S1SF003","S1SA302")),(11 to 20 ,Seq("S1SA302")))

  val rdd2 = sc.makeRDD(collect)

  val server = rdd2.preferredLocations(rdd2.partitions(0))
  println(server)

  val server2 =  rdd2.preferredLocations(rdd2.partitions(1))
  println(server2)*/


  //6.存储操作

/*  val file = sc.textFile("hdfs://S1SA300:8020/stat/000000_0")
  println(file.first())

  //7。转换操作

  val rdd = sc.makeRDD(1 to 5 ,1)
  val maordd = rdd.map(x => x.toFloat)
  val array = maordd.collect()

  for(a <- array){
    println(a)
  }

  val flatmaprdd = rdd.flatMap(x => (1 to x))

  println("first:" + flatmaprdd.first())

  val array2 = flatmaprdd.collect()
  for(a <- array2) println (a)



  val disticnt = flatmaprdd.distinct()
  val array3 = disticnt.collect()

  for(a <- array3) println ( "array3:"+a)*/


  //7.2.c重新分区
 /* val rdd = sc.makeRDD(1 to 10 ,100)
  val rerdd = rdd.repartition(4)

   val rdds = rerdd.partitions
  for(a <- rdds)   println(a)*/

  val rdd = sc.makeRDD(1 to 10 ,3)

  rdd.collect()








}


  def printlnArray(array: Array[String]):String ={
    val str = "";
    for(a <- array) {

    }
    return  null
  }



}
