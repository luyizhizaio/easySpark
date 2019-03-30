package opensource

import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by Kyrie on 2019/3/14.
 */
object MainTest {

  def main(args: Array[String]) {


    val sparkConf = new SparkConf().setAppName("FirstSpark").setMaster("local")
    //创建上下文 参数（spark集群地址，spark程序标示，spark安装路径，需要传入这个程序的jar包路径）
    val sc = new SparkContext(sparkConf)

    val rdd = sc.textFile("data/temp/text.txt")

    val count = rdd.count()

    println(count)

  }

}
