package com.mllib.example

import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by lichangyue on 2017/2/16.
 */
object SampleLinearRegression extends App{



  val conf = new SparkConf().setMaster("local").setAppName(this.getClass().getSimpleName().filter(!_.equals('$')))
  val sc = new SparkContext(conf)

  val sqlContext = new SQLContext(sc)

  //label,features
  // -9.490009878824548,(10,[0,1,2,3,4,5,6,7,8,9],[0.4551273600657362,0.36644694351969087,-0.38256108933468047,-0.4458430198517267,0.33109790358914726,0.8067445293443565,-0.2624341731773887,-0.44850386111659524,-0.07269284838169332,0.5658035575800715])
  val data = sqlContext.read.format("libsvm").load("data/mllib/sample_linear_regression_data.txt")

  val lr = new LinearRegression()
    .setMaxIter(10)
    .setRegParam(0.3)
    .setElasticNetParam(0.8)

  val df =  data.randomSplit(Array(0.8,0.2) ,seed = 100)
  val training = df(0)
  val testing = df(1)

  println("training:"+training.count())
  println("testing"+testing.count())

  //训练模型
  val  lrModel = lr.fit(data)



  println(s"Coefficients: ${lrModel.coefficients} Intercept: ${lrModel.intercept}")

  val trainingSummary = lrModel.summary
  println(s"numIterations: ${trainingSummary.totalIterations}")
  println(s"objectiveHistory: [${trainingSummary.objectiveHistory.mkString(",")}]")
  trainingSummary.residuals.show()
  println(s"RMSE: ${trainingSummary.rootMeanSquaredError}")
  println(s"r2: ${trainingSummary.r2}")



  //预测
  val predict= lrModel.transform(testing)
  predict.show(10)
//  +-------------------+--------------------+--------------------+
//  |              label|            features|          prediction|
//  +-------------------+--------------------+--------------------+
//  | -23.51088409032297|(10,[0,1,2,3,4,5,...| -1.2884561422688918|
//  |-22.837460416919342|(10,[0,1,2,3,4,5,...| -1.9304576033742633|
//  |-21.432387764165806|(10,[0,1,2,3,4,5,...| -0.1531130976360064|
//  |-20.212077258958672|(10,[0,1,2,3,4,5,...|  1.4999226024468593|

  sc.stop()

}
