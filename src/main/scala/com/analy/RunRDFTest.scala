package com.analy

import org.apache.spark.mllib.evaluation.MulticlassMetrics
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.tree.DecisionTree
import org.apache.spark.mllib.tree.model.DecisionTreeModel
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by dayue on 2017/3/24.
 */
object RunRDFTest {

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("RDF").setMaster("local")
    val sc = new SparkContext(conf)

    val rawData = sc.textFile("data/ana/covtype.data")

    val data = rawData.map{line =>
      val values = line.split(',').map(_.toDouble)
      //init 返回除最后一个值之外的所有值
      val featuresVector = Vectors.dense(values.init)
      val label = values.last -1
      LabeledPoint(label, featuresVector)
    }


    //评价指标使用精确度
    val Array (trainData, cvData ,testData) = data.randomSplit(Array(0.8,0.1,0.1))
    trainData.cache()
    cvData.cache()
    testData.cache()


    def getMetrics(model:DecisionTreeModel,data:RDD[LabeledPoint]):MulticlassMetrics={

      val predictionAndLabels = data.map(example =>
        (model.predict(example.features),example.label)
      )

      new MulticlassMetrics(predictionAndLabels)
    }

    val model = DecisionTree.trainClassifier(trainData,7,Map[Int,Int]() , "gini",4,100)

    val metrics = getMetrics(model, cvData)


    println(metrics.confusionMatrix)

    /*
    第 i 行第 j 列的元素代表一个正确类别为i的样本被预测为类别为 j 的次数。
    对角线上的元素代表预测正确的次数，而其他元素则代表预测错误的次数。 对角线上的次数多是好的
        1327.0  1313.0  0.0  0.0    10.0   2.0    46.0
        418.0   8100.0  0.0  0.0    41.0   14.0   4.0
        0.0     0.0     0.0  95.0   5.0    113.0  0.0
        0.0     0.0     0.0  200.0  0.0    13.0   0.0
        0.0     173.0   0.0  0.0    124.0  6.0    0.0
        0.0     0.0     0.0  61.0   20.0   124.0  0.0
        41.0    0.0     0.0  0.0    1.0    0.0    161.0
        */

    /**
     * 精确度
     * 用预测正确的样本数占整个样本数的比例来计算准确度
     * 精确度就是被标记为“ 正”而且确实是“正”的样本占所有标记为“正”的样本的比例
     * 召回率是被分类器标记为“正”的所有样本与所有本来就是“正”的样本的比率。
     */
    println(metrics.precision)
//    0.8116067903671536

    //多元分类的精确率和召回率，把每个类别单独视为“ 正”，所有其他类型视为负

    (0 until 7).map(
      cat => (metrics.precision(cat) , metrics.recall(cat))
    ).foreach(println)
    /*(0.7492778740612362,0.47613803230543317)
    (0.8427997113699619,0.949814126394052)
    (0.0,0.0)
    (0.5784061696658098,0.9615384615384616)
    (0.5678391959798995,0.42641509433962266)
    (0.4830188679245283,0.5871559633027523)
    (0.8817204301075269,0.7224669603524229)

在本例中，用一个多元分类的总体精确度就可以较好地度量分类准确度。
    */


    /**
     * 精确度的基准
     * 将所有类别在训练集和 CV集出现的概率相乘，然后把结果相加，我们就得到了一个对准确度的评估
     */

    def calssProbabilities(data:RDD[LabeledPoint]):Array[Double]={
      val countByuCategory = data.map(_.label).countByValue()  //（类别，样本数）
      val counts = countByuCategory.toArray.sortBy(_._1).map(_._2)
      counts.map(_.toDouble /counts.sum)  //各分类占总体的比例
    }

    val trainPriorProbabilities = calssProbabilities(trainData)
    val cvPriorProbabilities = calssProbabilities(cvData)
    val sum = trainPriorProbabilities.zip(cvPriorProbabilities).map{
      case (trainProb, cvProb)=> trainProb * cvProb

    }.sum

    println(sum)




























  }
}
