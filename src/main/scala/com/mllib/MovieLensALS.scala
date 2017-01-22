package com.mllib

import org.apache.spark.mllib.recommendation.{ALS, MatrixFactorizationModel, Rating}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, SparkConf}
import scala.io.Source

/**
 * Created by lichangyue on 2017/1/22.
 */
object MovieLensALS extends App {


  val conf = new SparkConf().setMaster("local").setAppName(this.getClass.getSimpleName)

  val sc = new SparkContext(conf)

//  val path = "data/mllib/collaborativefilter/ratings.dat"
//  val myRatings = loadRatings(path)
//  val myRatingsRDD = sc.parallelize(myRatings,1)


  //装载样本评分数据，其中最后一列Timestamp取除10的余数作为key，Rating为值，即(Int，Rating)
  val ratings = sc.textFile("data/mllib/collaborativefilter/ratings.dat").map{line=>
    val fields = line.split("::")
    //  用户ID::电影ID::评分::时间
    (fields(3).toLong % 10 , Rating(fields(0).toInt,fields(1).toInt, fields(2).toDouble))
  }


  //装载电影目录对照表(电影ID->电影标题)
  val movies = sc.textFile("data/mllib/collaborativefilter/movies.dat").map{line =>
    val fields = line.split("::")
//    电影ID::电影名称::电影种类
    (fields(0).toInt,fields(1))

  }.collect().toMap

  //统计有用户数量和电影数量以及用户对电影的评分数目
  val numRatings = ratings.count()
  val numUsers = ratings.map(_._2.user).distinct().count()
  val numMovies = ratings.map(_._2.product).distinct().count()
  println("Got " + numRatings + " ratings from " + numUsers + " users " + numMovies + " movies")


  //将样本评分表以key值切分成3个部分，分别用于训练 (60%，并加入用户评分), 校验 (20%), and 测试 (20%)

  val raw =ratings.values.randomSplit(Array(0.6,0.2,0.2),seed = 10)

  val training = raw(0)
  val validation = raw(1)
  val test = raw(2)

  val numTraining = training.count()

  val numValidation = validation.count()

  val numTest = test.count()

  println("Training: " + numTraining + " validation: " + numValidation + " test: " + numTest)


  //训练不同参数下的模型，并在校验集中验证，获取最佳参数下的模型

  val ranks =List(8,12)
  val lambdas = List(0.1,10.0)
  val numIters = List(10,20)
  var bestModel :Option[MatrixFactorizationModel] = None
  var bestValidationRmse = Double.MaxValue
  var bestRank = 0
  var bestLambda = -1.0
  var bestNumIter = -1

  for (rank <-ranks;lambda <-lambdas; numIter <- numIters){

    val model = ALS.train(training,rank,numIter ,lambda)
    val validationRmse = computeRmse(model,validation,numValidation)
    println("RMSE(validation) = " + validationRmse + " for the model trained with rank = "
      + rank + ",lambda = " + lambda + ",and numIter = " + numIter + ".")

    if(validationRmse < bestValidationRmse){

      bestModel = Some(model)
      bestValidationRmse = bestValidationRmse
      bestRank = rank
      bestLambda = lambda
      bestNumIter = numIter
    }
  }
  //用最佳模型预测测试集的评分，并计算和实际评分之间的均方根误差（RMSE）
  val testRmse = computeRmse(bestModel.get,test,numTest)

  println("The best model was trained with rank=" + bestRank + "and lambda ="+bestLambda
    +",and numIter = " +bestNumIter + ",and its RMSE on the test set is "+testRmse+ ".")

  //create a naive baseline and compare it with the best model
  val meanRating = training.union(validation).map(_.rating).mean() //评分的均值

  val baselineRmse = math.sqrt(test.map(x =>(meanRating - x.rating) * (meanRating - x.rating)).reduce(_+_)/numTest)

  val improvement = (baselineRmse - testRmse) / baselineRmse *100
  println("The best model improves the baseline by " + "%1.2f".format(improvement) +"%.")


  //推荐前十部最感兴趣的电影，注意要剔除用户已经评分的电影
  val myRatedMovieIds =





  sc.stop()







  /** 校验集预测数据和实际数据之间的均方根误差 **/

  def computeRmse(model:MatrixFactorizationModel, data:RDD[Rating],n:Long):Double ={

    val predictions:RDD[Rating] = model.predict(data.map(x =>(x.user,x.product)))

    val predictionsAndRatings = predictions.map{x => ((x.user,x.product),x.rating)}
      .join(data.map(x=>((x.user,x.product),x.rating))).values
    math.sqrt(predictionsAndRatings.map(x=> (x._1 - x._2) * (x._1 - x._2)).reduce(_+_)/n)

  }

  /** 装载用户评分文件 personalRatings.txt **/
//  1::1193::5::978300760
//  1::661::3::978302109
  def loadRatings(path:String):Seq[Rating] ={

    val lines= Source.fromFile(path).getLines
    val ratings = lines.map{line =>
      val fields = line.split("::")
      Rating(fields(0).toInt,fields(1).toInt,fields(2).toDouble)
    }.filter(_.rating > 0.0)

    if(ratings.isEmpty){
      sys.error("No ratings provided")
    } else {
      ratings.toSeq
    }
  }

}
