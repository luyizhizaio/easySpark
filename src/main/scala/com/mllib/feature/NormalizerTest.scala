package com.mllib.feature

import org.apache.spark.mllib.feature.Normalizer
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by lichangyue on 2017/3/8.
 */
object NormalizerTest {

  def main(args: Array[String]) {


    val conf = new SparkConf().setAppName(this.getClass.getSimpleName).setMaster("local")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)


    val data = MLUtils.loadLibSVMFile(sc, "data/mllib/sample_libsvm_data.txt")
    //默认情况下，p=2。计算2阶范数
    val normalizer1 = new Normalizer()

    val normalizer2 = new Normalizer(p=Double.PositiveInfinity)

    val data1= data.map(x => (x.label,normalizer1.transform(x.features)))


    val data2 = data.map(x => (x.label,normalizer2.transform(x.features)))


    data1.take(3).foreach(println)
    println("--------------------")

    data2.take(3).foreach(println)







  }

}
