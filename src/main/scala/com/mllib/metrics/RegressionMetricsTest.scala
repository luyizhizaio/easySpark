package com.mllib.metrics

import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.mllib.regression.LinearRegressionWithSGD
import org.apache.spark.mllib.evaluation.RegressionMetrics
import org.apache.spark.mllib.util.MLUtils

/**
 * Created by lichangyue on 2017/3/14.
 */
object RegressionMetricsTest {



  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("regression").setMaster("local[1]")
    val sc = new SparkContext(conf)



    // Load the data
    val data = MLUtils.loadLibSVMFile(sc, "data/mllib/sample_linear_regression_data.txt").cache()

    // Build the model
    val numIterations = 100
    val model = LinearRegressionWithSGD.train(data, numIterations)

    // Get predictions
    val valuesAndPreds = data.map{ point =>
      val prediction = model.predict(point.features)
      (prediction, point.label)
    }

    // Instantiate metrics object
    val metrics = new RegressionMetrics(valuesAndPreds)

    // Squared error
    println(s"MSE = ${metrics.meanSquaredError}")
    println(s"RMSE = ${metrics.rootMeanSquaredError}")

    // R-squared
    println(s"R-squared = ${metrics.r2}")

    // Mean absolute error
    println(s"MAE = ${metrics.meanAbsoluteError}")

    // Explained variance
    println(s"Explained variance = ${metrics.explainedVariance}")

//    MSE = 103.30968681818085
//    RMSE = 10.164137288436281
//    R-squared = 0.027639110967837
//    MAE = 8.148691907953312
//    Explained variance = 2.8883952017178958


  }






}
