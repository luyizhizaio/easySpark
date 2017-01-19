package com.mllib

import breeze.linalg.sum
import org.apache.spark.graphx.lib.LabelPropagation
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.{LinearRegressionWithSGD, LabeledPoint}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by lichangyue on 2017/1/18.
 */
object LinearRegrTest {

  def main(args: Array[String]) {


    val conf=new SparkConf().setAppName("regression").setMaster("local[1]")
    val sc=new SparkContext(conf)

    val records=sc.textFile("data/mllib/hour.csv").map(_.split(",")).cache()


    //获取records的第2到10个字段调用get_mapping
    val mappings = for(i <-Range(2,10)) yield get_mapping(records ,i)

    val cat_len = sum(mappings.map(_.size)) //获取离散型变量的长度
    val num_len = records.first().slice(10,14).size   //slice方法截取数组
    val total_len = cat_len + num_len

    //linear regression data 此部分代码最重要，主要用于产生训练数据集，按照前文所述处理类别特征和实数特征。

    val data = records.map{record =>
      val cat_vec = Array.ofDim[Double](cat_len)
      var i =0
      var step = 0
      for (filed <- record.slice(2,10)){
        val m = mappings(i)
        val idx = m(filed)
        cat_vec(idx.toInt +step) = 1.0
        i +=1
        step = step + m.size
      }
      val num_vec = record.slice(10,14).map(x => x.toDouble)

      val features = cat_vec ++ num_vec
      val label = record(record.size -1).toInt

        LabeledPoint(label , Vectors.dense(features))
    }
    val categoricalFeaturesInfo = Map[Int,Int]()

    /**
     * 返回模型
     * * @param stepSize Step size to be used for each iteration of Gradient Descent.
     * * @param numIterations Number of iterations of gradient descent to run.
     */
    val linear_model = LinearRegressionWithSGD.train(data,10,0.5)

    //预测结果
    val true_vs_predicted = data.map(p => (p.label, linear_model.predict(p.features)))

    println(true_vs_predicted.take(5).toVector.toString())



  }


  def get_mapping(rdd:RDD[Array[String]],idx:Int)={

    //获取数组中指定的索引的元素值，值和索引合并，再变成一个map
    rdd.map(filed => filed(idx)).distinct().zipWithIndex().collectAsMap()
  }
}
