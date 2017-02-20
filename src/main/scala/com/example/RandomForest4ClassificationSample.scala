package com.example

import org.apache.spark.mllib.tree.RandomForest
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.mllib.util.MLUtils

/**
 * Created by lichangyue on 2017/2/17.
 */
object RandomForest4ClassificationSample extends App {


  val conf = new SparkConf().setMaster("local").setAppName(this.getClass().getSimpleName().filter(!_.equals('$')))
  val sc = new SparkContext(conf)

  val sqlContext = new SQLContext(sc)
  val data = MLUtils.loadLibSVMFile(sc , "data/mllib/sample_libsvm_data.txt")


  val split = data.randomSplit(Array(0.7,0.3))
  val (trainingData, testData) = (split(0),split(1))

  // Train a RandomForest model.
  // 空的类别特征信息表示所有的特征都是连续的.
  val numClasses = 2
  val categoricalFeaturesInfo = Map[Int, Int]()
  val numTrees = 3 // Use more in practice.
  val featureSubsetStrategy = "auto" // Let the algorithm choose.
  val impurity = "gini"
  val maxDepth = 4
  val maxBins = 32

  val model = RandomForest.trainClassifier(trainingData,numClasses,
    categoricalFeaturesInfo,numTrees,featureSubsetStrategy,impurity,maxDepth,maxBins)
  // Evaluate model on test instances and compute test error
  val labelAndPreds = testData.map{point=>
    val prediction = model.predict(point.features)
    (point.label,prediction)
  }

  val testErr = labelAndPreds.filter(r => r._1 != r._2).count.toDouble / testData.count()
  println("Test Error = " + testErr)
  println("Learned classification forest model:\n" + model.toDebugString) //输出树具体规则


  sc.stop()
}
