package opensource.rdd.dependency

import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by Kyrie on 2019/3/30.
 */
object RangeDep {

  def main(args: Array[String]) {


    val sparkConf = new SparkConf().setAppName("FirstSpark").setMaster("local")
    //创建上下文 参数（spark集群地址，spark程序标示，spark安装路径，需要传入这个程序的jar包路径）
    val sc = new SparkContext(sparkConf)

    val rdd1 = sc.textFile("data/temp/1.txt")
    val rdd2 = sc.textFile("data/temp/text.txt")

    val rdd3=rdd1.union(rdd2)
    rdd2.count


  }

}
