package com.example

import org.apache.spark.mllib.tree.DecisionTree
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.mllib.util.MLUtils

/**
 * Created by lichangyue on 2017/2/17.
 */
object DecisionTree4Regression extends App {

  val conf = new SparkConf().setMaster("local").setAppName(this.getClass().getSimpleName().filter(!_.equals('$')))
  val sc = new SparkContext(conf)

  val sqlContext = new SQLContext(sc)

  val data = MLUtils.loadLibSVMFile(sc ,"data/mllib/sample_libsvm_data.txt" )

  val split = data.randomSplit(Array(0.7,0.3))
  val (trainingData, testData) = (split(0),split(1))

  val categoricalFeaturesInfo = Map[Int, Int]()
  val impurity = "variance"
  val maxDepth = 5
  val maxBins = 32

  val model = DecisionTree.trainRegressor(trainingData,categoricalFeaturesInfo,
    impurity,maxDepth,maxBins)

  val labelAndPredictions = testData.map{point =>
    val prediction = model.predict(point.features)
    (point.label, prediction)
  }

  labelAndPredictions.take(10).foreach(println)

  //math.pow（a，2） 幂运算 a的平方
  val testMSE = labelAndPredictions.map{ case (v, p) => math.pow(v - p, 2) }.mean()
  println("Test Mean Squared Error = " + testMSE)
  println("Learned regression tree model:\n" + model.toDebugString)


}
