package com.ml.one.TransformerAndEstimaterAndPipeline
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.ml.tuning.{ParamGridBuilder, TrainValidationSplit}
/**
 * Created by lichangyue on 2017/1/3.
 */
object MyTrainValidationSplit {

  def main(args: Array[String]) {

    // Prepare training and test data.
    //准备训练数据和测试数据
    val data = sqlContext.read.format("libsvm").load("data/mllib/sample_linear_regression_data.txt")
    val Array(training, test) = data.randomSplit(Array(0.9, 0.1), seed = 12345)

    val lr = new LinearRegression()

    // We use a ParamGridBuilder to construct a grid of parameters to search over.
    // TrainValidationSplit will try all combinations of values and determine best model using
    // the evaluator.
    //ParamGridBuilder构建一组参数
    //TrainValidationSplit将尝试从这些所有值的组合中使用evaluator选出最好的模型
    val paramGrid = new ParamGridBuilder()
      .addGrid(lr.regParam, Array(0.1, 0.01))
      .addGrid(lr.fitIntercept)
      .addGrid(lr.elasticNetParam, Array(0.0, 0.5, 1.0))
      .build()

    // In this case the estimator is simply the linear regression.
    // A TrainValidationSplit requires an Estimator, a set of Estimator ParamMaps, and an Evaluator.
    //在这里estimator是简单的线性回归
    //TrainValidationSplit 需要一个Estimator ， 一个Estimator ParamMaps集，一个Evaluator
    val trainValidationSplit = new TrainValidationSplit()
      .setEstimator(lr)
      .setEvaluator(new RegressionEvaluator)
      .setEstimatorParamMaps(paramGrid)
      // 80% of the data will be used for training and the remaining 20% for validation.
      //80%数据作为训练，剩下的20%作为验证
      .setTrainRatio(0.8)

    // Run train validation split, and choose the best set of parameters.
    //运行训练校验分离，选择最好的参数。
    val model = trainValidationSplit.fit(training)

    // Make predictions on test data. model is the model with combination of parameters
    // that performed best.
    //在测试数据上做预测，模型是参数组合中执行最好的一个
    model.transform(test)
      .select("features", "label", "prediction")
      .show()
  }

}


