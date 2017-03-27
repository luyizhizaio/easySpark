package com.analysis

import org.apache.spark.mllib.evaluation.MulticlassMetrics
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.tree.{RandomForest, DecisionTree}
import org.apache.spark.mllib.tree.model.DecisionTreeModel
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by lichangyue on 2017/3/27.
 * 第四章决策树和随机森林
 */
object RunRDFTest {

  def main(args: Array[String]) {

    val sc = new SparkContext(new SparkConf().setAppName("RDF").setMaster("local"))

    val rawData = sc.textFile("data/analy/covtype.data")

    val data = rawData.map{line=>
      val values = line.split(',').map(_.toDouble)
      val featuresVector = Vectors.dense(values.init)//init返回除了最后一个元素以外的所有元素
      val label= values.last -1  //决策树的label从0开始
      LabeledPoint(label,featuresVector)
    }

    //训练集，cv集，test集
    val Array(trainData,cvData, testData) = data.randomSplit(Array(0.8,0.1,0.1))
    trainData.cache()
    cvData.cache()
    testData.cache()

    //简单的分类器
    simpleDecisionTree(trainData,cvData)

    //随机分类，作为算法分类器的基准
    randomClassifier(trainData,cvData)
    //交叉校验，选择模型
    evaluate(trainData,cvData,testData)
    //把one-hot转成分类类型
    evaluateCategorical(rawData)
    //随机森林
    evaluateForest(rawData)



    trainData.unpersist()
    cvData.unpersist()
    testData.unpersist()

  }

  def simpleDecisionTree(trainData:RDD[LabeledPoint] ,cvData: RDD[LabeledPoint]):Unit = {

    val model = DecisionTree.trainClassifier(trainData, 7 , Map[Int,Int](),"gini" ,4 ,100)


    val metrics =  getMetrics(model,cvData)

    println(metrics.confusionMatrix)//输出混合矩阵

    println(metrics.precision) //总体的精确度
//    0.7874891493055556

    //查看每种分类的精确率和召回率率
    (0 until 7).map(
      category => (metrics.precision(category), metrics.recall(category))
    ).foreach(println)

  }

  /**
   * 度量方法
   * @param model
   * @param data
   * @return
   */
  def getMetrics(model : DecisionTreeModel,data:RDD[LabeledPoint]):MulticlassMetrics = {

    val predictionAndLabels = data.map{example =>
      (model.predict(example.features),example.label)
    }
    new MulticlassMetrics(predictionAndLabels)
  }


  /**
   * 随机分类精确度
   * @param trainData
   * @param cvData
   */
  def  randomClassifier(trainData:RDD[LabeledPoint] ,cvData: RDD[LabeledPoint]): Unit = {
    //按照类别在训练集中出现的比例来预测类别
    val  trainPriorProbabilities = classProbabilities(trainData)
    val cvPriorProbabilities = classProbabilities(cvData)

    //把测试集和训练集的比例相乘再加权，得到随机分类精确度
    val accuracy = trainPriorProbabilities.zip(cvPriorProbabilities).map{ //zip 合并两个Array，形成对
      case(trainProb, cvProb) => trainProb * cvProb
    }.sum

    println(accuracy)
  }



  def classProbabilities(data: RDD[LabeledPoint]):Array[Double] ={

    //计算各分类的数量
    val countsByCategory = data.map(_.label).countByValue()  //返回（类型，数量）
    val counts = countsByCategory.toArray.sortBy(_._1).map(_._2)

    counts.map(_.toDouble / counts.sum)

  }


  def evaluate(
     trainData:RDD[LabeledPoint] ,
     cvData:RDD[LabeledPoint],
     testData:RDD[LabeledPoint]): Unit ={

    val evalutations =
      for (impurity <- Array("gini","entropy");
            depth <- Array(1,20);
            bins <- Array(10,300))
        yield{
          val model = DecisionTree.trainClassifier(
              trainData,7,Map[Int,Int](),impurity,depth,bins)

          val accuracy = getMetrics(model, cvData).precision

          ((impurity,depth,bins), accuracy)
        }

    evalutations.sortBy(_._2).reverse.foreach(println)

    //选出最好的参数(entropy,20,300)，(trainData +vcData)重新训练模型，然后在测试集上评估

    val model = DecisionTree.trainClassifier(
    trainData.union(cvData) ,7,Map[Int,Int](), "entropy",20 ,300)

    println(getMetrics(model,testData).precision)
    println(getMetrics(model,trainData.union(cvData)).precision)
  }

  def evaluateCategorical(rawData:RDD[String]): Unit ={
    val data = unencodeOneHot(rawData)

    val Array(trainData, cvData,testData) = data.randomSplit(Array(0.8,0.1,0.1))
    trainData.cache()
    cvData.cache()
    testData.cache()


    val  evaluations =
      for(impurity <- Array("gini","entropy");
          depth <-Array(10 , 20 ,30);
          bins <-Array(40,300))
      yield{
        val model = DecisionTree.trainClassifier(
          trainData,7, Map(10->4, 11->40),impurity,depth,bins)
        val trainAccuracy = getMetrics(model,trainData).precision
        val cvAccuracy = getMetrics(model,cvData).precision

        ((impurity,depth,bins),(trainAccuracy,cvAccuracy))
      }

    evaluations.sortBy(_._2._2).reverse.foreach(println)

    val model = DecisionTree.trainClassifier(
      trainData.union(cvData), 7 ,Map(10->4,11->40),"entropy", 30,300)

    println(getMetrics(model,testData).precision)

    trainData.unpersist()
    cvData.unpersist()
    testData.unpersist()
  }

  /**
   * 去除oneHot编码
   * @param rawData
   * @return
   */
  def unencodeOneHot(rawData :RDD[String]):RDD[LabeledPoint] = {
    rawData.map{line =>
      val values = line.split(',').map(_.toDouble)
      //wilderness
      val wilderness = values.slice(10,14).indexOf(1.0).toDouble //indexOf返回1.0的下标
      //soil
      val soil = values.slice(14,54).indexOf(1.0).toDouble
      val featureVector = Vectors.dense(values.slice(0,10) :+wilderness :+ soil)
      val label = values.last - 1
      LabeledPoint(label,featureVector)
    }
  }

  def evaluateForest(rawData:RDD[String]): Unit ={

    val data = unencodeOneHot(rawData)
    val Array(trainData,cvData) = data.randomSplit(Array(0.9,0.1))
    trainData.cache()
    cvData.cache()

    val forest = RandomForest.trainClassifier(
    trainData,7,Map(10->4,11->40),20,"auto","entropy",30,300)


    val predictionsAndLabels = cvData.map(example =>
      (forest.predict(example.features),example.label)
    )
    println(new MulticlassMetrics(predictionsAndLabels).precision)



    //预测
    val input ="2709,125,28,67,23,3224,253,207,61,6094,0,29"
    val vector = Vectors.dense(input.split(',').map(_.toDouble))
    println("预测结果:"+forest.predict(vector))

  }

}
