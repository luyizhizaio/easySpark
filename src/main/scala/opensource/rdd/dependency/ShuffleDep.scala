package opensource.rdd.dependency

import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by Kyrie on 2019/3/30.
 */
object ShuffleDep {


  def main(args: Array[String]) {


    val sparkConf = new SparkConf().setAppName("FirstSpark").setMaster("local")
    //创建上下文 参数（spark集群地址，spark程序标示，spark安装路径，需要传入这个程序的jar包路径）
    val sc = new SparkContext(sparkConf)

    val rdd1 = sc.textFile("data/temp/1.txt")


    val rdd2 = rdd1.map{line =>
      val Array(k,v) = line.split(" ",2)
      k->v
    }.reduceByKey(_+ "," +_)
    rdd2.count


  }

}
