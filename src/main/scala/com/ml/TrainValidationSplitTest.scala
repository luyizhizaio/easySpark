package com.ml

import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.ml.tuning.{TrainValidationSplit, ParamGridBuilder}
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by lichangyue on 2017/3/16.
 */
object TrainValidationSplitTest {


  def main(args: Array[String]) {
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR);
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.ERROR);


    val conf = new SparkConf().setAppName("decisionTree").setMaster("local[2]")


    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    val data = sqlContext.read.format("libsvm").load("data/mllib/sample_linear_regression_data.txt")
    val Array(training, test) = data.randomSplit(Array(0.9, 0.1), seed = 12345)

    val lr = new LinearRegression()

    // We use a ParamGridBuilder to construct a grid of parameters to search over.
    // TrainValidationSplit will try all combinations of values and determine best model using
    // the evaluator.
    val paramGrid = new ParamGridBuilder()
      .addGrid(lr.regParam, Array(0.1, 0.01))
      .addGrid(lr.fitIntercept)
      .addGrid(lr.elasticNetParam, Array(0.0, 0.5, 1.0))
      .build()

    // In this case the estimator is simply the linear regression.
    // A TrainValidationSplit requires an Estimator, a set of Estimator ParamMaps, and an Evaluator.
    val trainValidationSplit = new TrainValidationSplit()
      .setEstimator(lr)
      .setEvaluator(new RegressionEvaluator)
      .setEstimatorParamMaps(paramGrid)
      // 80% of the data will be used for training and the remaining 20% for validation.
      .setTrainRatio(0.8)

    // Run train validation split, and choose the best set of parameters.
    val model = trainValidationSplit.fit(training)


    // Make predictions on test data. model is the model with combination of parameters
    // that performed best.
    model.transform(test)
      .select("features", "label", "prediction")
      .show()

//    |            features|              label|          prediction|
//    +--------------------+-------------------+--------------------+
//    |(10,[0,1,2,3,4,5,...|-22.837460416919342| -3.0538822810698187|
//      |(10,[0,1,2,3,4,5,...|-19.782762789614537|  0.4257348398153445|
//      |(10,[0,1,2,3,4,5,...| -8.772667465932606| -0.9323599838153458|
//      |(10,[0,1,2,3,4,5,...| -6.397510534969392|   1.124610396303996|
//      |(10,[0,1,2,3,4,5,...| -5.249715067336168|   3.437303357322573|
//      |(10,[0,1,2,3,4,5,...| -5.090863544403131|  -0.591307868133208|
//      |(10,[0,1,2,3,4,5,...| -4.438869807456516| 0.48149737135008813|
//      |(10,[0,1,2,3,4,5,...| -2.203945173593806| -3.0673972530769955|

  }


}
