package com.example

import org.apache.spark.mllib.tree.RandomForest
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by lichangyue on 2017/2/17.
 */
object RandomForest4RegressionSample extends App {


  val conf = new SparkConf().setMaster("local").setAppName(this.getClass().getSimpleName().filter(!_.equals('$')))
  val sc = new SparkContext(conf)

  val sqlContext = new SQLContext(sc)
  val data = MLUtils.loadLibSVMFile(sc , "data/mllib/sample_libsvm_data.txt")


  val split = data.randomSplit(Array(0.7,0.3))
  val (trainingData, testData) = (split(0),split(1))

  // Train a RandomForest model.
  // 空的类别特征信息表示所有的特征都是连续的.
  val categoricalFeaturesInfo = Map[Int, Int]()
  val numTrees = 3 // Use more in practice.
  val featureSubsetStrategy = "auto" // Let the algorithm choose.
  val impurity = "variance"  //不纯度，回归只能用variance
  val maxDepth = 4
  val maxBins = 32

  val model = RandomForest.trainRegressor(trainingData,
    categoricalFeaturesInfo,numTrees,featureSubsetStrategy,impurity,maxDepth,maxBins)
  // Evaluate model on test instances and compute test error
  val labelsAndPredictions = testData.map{point=>
    val prediction = model.predict(point.features)
    (point.label,prediction)
  }

  val testMSE = labelsAndPredictions.map{ case(v, p) => math.pow((v - p), 2)}.mean()
  println("Test Mean Squared Error = " + testMSE)
  println("Learned regression forest model:\n" + model.toDebugString)


  sc.stop()
}
