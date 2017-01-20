package com.mllib

import org.apache.spark.mllib.classification.SVMWithSGD
import org.apache.spark.mllib.evaluation.MulticlassMetrics
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.mllib.util.MLUtils

/**
 * Created by lichangyue on 2017/1/19.
 */
object SVMTest {

  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName(this.getClass.getSimpleName).setMaster("local")
    val sc = new SparkContext(conf)

    //加载数据
    val path = "data/mllib/sample_libsvm_data.txt"
    val examples = MLUtils.loadLibSVMFile(sc, path)

    //划分测试样本和训练样本
    val splits = examples.randomSplit(Array(0.6,0.4),seed=11L)
    val trains = splits(0)
    val test  = splits(1)
    //3 新建SVM模型，并设置训练参数
    val numlterations = 1000
    val stepSize = 1
    val miniBatchFraction = 1.0

    val model = SVMWithSGD.train(trains, numlterations, stepSize,miniBatchFraction)

    //对测试数据预测
    val prediction = model.predict(test.map(_.features))

    val predictionAndLabel = prediction.zip(test.map(_.label))

    //计算误差
    val metrics = new MulticlassMetrics(predictionAndLabel)
    val precision = metrics.precision
    println("precision" + precision)

    sc.stop()
  }

}
