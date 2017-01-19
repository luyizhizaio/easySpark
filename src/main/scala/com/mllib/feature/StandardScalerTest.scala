package com.mllib.feature

import org.apache.spark.mllib.feature.{StandardScalerModel, StandardScaler}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.mllib.util.MLUtils

/**
 * Created by lichangyue on 2017/1/18.
 * 标准化
 */
object StandardScalerTest {

  def main(args: Array[String]) {


    val conf = new SparkConf().setMaster("local").setAppName(this.getClass.getSimpleName)
    val sc = new SparkContext(conf)

    val data = MLUtils.loadLibSVMFile(sc,"data/mllib/sample_libsvm_data.txt")

    val scaler1 = new StandardScaler().fit(data.map(x => x.features))
    val scaler2 = new StandardScaler(withMean = true, withStd =true).fit(data.map(x => x.features))
    // scaler3 is an identical model to scaler2, and will produce identical transformations
    val scaler3 = new StandardScalerModel(scaler2.std, scaler2.mean)

    val data1 = data.map(x =>(x.label , scaler1.transform(x.features)))
    data1.take(10).foreach(println)

    val data2 = data.map(x => (x.label, scaler2.transform(Vectors.dense(x.features.toArray))))
    println("scaler2")
    data2.take(10).foreach(println)

  }

}
