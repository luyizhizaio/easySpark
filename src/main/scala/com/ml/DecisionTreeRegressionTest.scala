package com.ml

import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.feature.VectorIndexer
import org.apache.spark.ml.regression.{DecisionTreeRegressionModel, DecisionTreeRegressor}
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by lichangyue on 2017/3/16.
 */
object DecisionTreeRegressionTest {

  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName(this.getClass.getSimpleName).setMaster("local")

    val sc = new SparkContext(conf)

    val sqlContext = new SQLContext(sc)

    val data = sqlContext.read.format("libsvm").load("data/mllib/sample_libsvm_data.txt")


    //自动识别离散型数值

    val featuresIndexer = new VectorIndexer()
    .setInputCol("features")
    .setOutputCol("indexedFeatures")
    .setMaxCategories(4)
    .fit(data)

    //分离测试和训练数据
    val Array(trainingData , testData) = data.randomSplit(Array(0.7,0.3))


    val dt = new DecisionTreeRegressor()
    .setLabelCol("label")
    .setFeaturesCol("indexedFeatures")

    //管道链
    val pipeline = new Pipeline()
    .setStages(Array(featuresIndexer,dt))

    //训练模型
    val model = pipeline.fit(trainingData)

    //预测
    val predictions = model.transform(testData)


    predictions.select("prediction","label","features").show(5)


    //错误度量
    val evaluator = new RegressionEvaluator()
    .setLabelCol("label")
    .setPredictionCol("prediction")
    .setMetricName("rmse")  //取值：mse|rmse|r2|mae

    val rmse = evaluator.evaluate(predictions)

    println("Root Mean Squared Error (RMSE) on test data = " + rmse)
//    Root Mean Squared Error (RMSE) on test data = 0.18257418583505536

    val  treeModel = model.stages(1).asInstanceOf[DecisionTreeRegressionModel]


    println("learned regression tree model:\n" + treeModel.toDebugString)


//    learned regression tree model:
//      DecisionTreeRegressionModel (uid=dtr_b63db0e8de34) of depth 2 with 5 nodes
//      If (feature 406 <= 72.0)
//    If (feature 99 in {0.0,3.0})
//    Predict: 0.0
//    Else (feature 99 not in {0.0,3.0})
//    Predict: 1.0
//    Else (feature 406 > 72.0)
//    Predict: 1.0


    sc.stop()
  }

}
