package com.analysis

import org.apache.spark.mllib.clustering.{KMeansModel, KMeans}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by lichangyue on 2017/3/27.
 */
object RunKmeansTest {


  def main(args: Array[String]) {

    val sc = new SparkContext(
      new SparkConf().setAppName(this.getClass.getSimpleName).setMaster("local"))
    //    0,tcp,http,SF,217,2032,0,0,0,0,0,1,0,0,0,0,0,0,0,0,0,0,6,6,0.00,0.00,0.00,0.00,1.00,0.00,0.00,49,49,1.00,0.00,0.02,0.00,0.00,0.00,0.00,0.00,normal.
    val rawData = sc.textFile("data/analy/kddcup.data")

    //简单聚类
//    simpleKeans(rawData)

    //指定不同的k，计算聚类评估的分数
//    clusterTake1(rawData)

    //标准化数据后聚类
//    clusterTake2(rawData)

    //处理分类类型
    clusterTake3(rawData)

    //使用熵度量
    clusterTake4(rawData)

    //异常检测
    anomalies(rawData)

  }


  def simpleKeans(rawData: RDD[String]): Unit = {
    //类别标号以及每类样本有多少
    rawData.map(_.split(',').last).countByValue().toSeq.sortBy(_._2)
      .reverse.foreach(println)



    val labelsAndData = rawData.map { line =>
      val buffer = line.split(',').toBuffer // toBuffer 创建一个Buffer，它是一个可变列表
      buffer.remove(1, 3) //移除字符类型
    val label = buffer.remove(buffer.length - 1)
      val vector = Vectors.dense(buffer.map(_.toDouble).toArray)

      (label, vector)
    }

    val data = labelsAndData.values.cache()
    val kmeans = new KMeans()
    val model = kmeans.run(data)

    //输出所有质心,默认是两个质心
    model.clusterCenters.foreach(println)


    val clusterLabelCount = labelsAndData.map { case (label, data) =>

      val cluster = model.predict(data)
      (cluster, label)
    }.countByValue() //统计元组对应的数量

    //输出相同簇和相同标签的数量
    clusterLabelCount.toSeq.sorted.foreach { case ((cluster, label), count) =>
      println(f"$cluster%1s$label%18s$count%8s")
    }
    data.unpersist()

  }

  def clusterTake1(rawData: RDD[String]): Unit = {

    val data = rawData.map { line =>
      val buffer = line.split(',').toBuffer //转成可变数组
      buffer.remove(1, 3)
      buffer.remove(buffer.length - 1)
      Vectors.dense(buffer.map(_.toDouble).toArray)
    }.cache()

    //返回k个聚类和对应聚类分数， 分数越低越好
    (5 to 30 by 5).map(k => (k, clusteringScore(data, k))).foreach(println)

    //训练模型时增加了调参  ; par方法返回Range的并行实现
    (30 to 100 by 10).par.map(k =>
      (k, clusteringScore2(data, k))
    ).toList.foreach(println)

    data.unpersist()

  }

  /**
   * 计算聚类的分数
   * @param data
   * @param k
   * @return
   */
  def clusteringScore(data: RDD[Vector], k: Int): Double = {
    val kmeans = new KMeans()
    kmeans.setK(k) //设置簇数量
    val model = kmeans.run(data) //预测出模型
    data.map(datum => distToCentroid(datum, model)).mean() //计算数据到所属质心距离的均值
  }

  /**
   * 计算聚类的分数
   * @param data
   * @param k
   * @return
   */
  def clusteringScore2(data: RDD[Vector], k: Int): Double = {
    val kmeans = new KMeans()
    kmeans.setK(k) //设置簇数量
    kmeans.setRuns(10) //设置在给定k 值时运行的次数
    kmeans.setEpsilon(1.0e-6) //该阈值控制聚类过程中簇质心进行有效移动的最小值。
    val model = kmeans.run(data) //预测出模型
    data.map(datum => distToCentroid(datum, model)).mean() //计算数据到所属质心距离的均值
  }

  /**
   * 返回该向量到所属质心的欧式距离
   * @param datum
   * @param model
   * @return
   */
  def distToCentroid(datum: Vector, model: KMeansModel) = {

    val cluster = model.predict(datum)
    val centroid = model.clusterCenters(cluster) //获取指定簇的质心
    distance(centroid, datum)
  }

  /**
   * 计算两个向量的欧式距离
   * @param a
   * @param b
   */
  def distance(a: Vector, b: Vector) = {
    //差的绝对值平方和开根号
    math.sqrt(a.toArray.zip(b.toArray).map(p => p._1 - p._2).map(d => d * d).sum)
  }

  def clusterTake2(rawData: RDD[String]): Unit = {

    val data = rawData.map { line =>
      val buffer = line.split(',').toBuffer
      buffer.remove(1, 3)
      buffer.remove(buffer.length - 1)
      Vectors.dense(buffer.map(_.toDouble).toArray)
    }
    //标准化数值型数据
    val normalizedData = data.map(buildNormalizationFuncation(data)).cache()

    (60 to 120 by 10 ).par.map(k =>
      (k ,clusteringScore2(normalizedData,k))).toList.foreach(println)

    normalizedData.unpersist()


  }

  /**
   * 标准化数据, 这个方法返回的也是一个方法
   * @param data
   * @return
   */
  def buildNormalizationFuncation(data: RDD[Vector]): (Vector => Vector) = {

    val dataAsArray = data.map(_.toArray) //向量转成数组
    val numCols = dataAsArray.first().length //列数量

    val n = dataAsArray.count() //总记录数
    //把数组的所有列加起来
    val sums = dataAsArray.reduce(
        (a, b) => a.zip(b).map(t => t._1 + t._2))

    //得到各列元素的平方和
    val sumSquares = dataAsArray.aggregate(
      new Array[Double](numCols)
    )(
        (a, b) => a.zip(b).map(t => t._1 + t._2 * t._2), //
        (a, b) => a.zip(b).map(t => t._1 + t._2)
      )


    val stdevs = sumSquares.zip(sums).map {
      case (sumSq, sum) => math.sqrt(n * sumSq - sum * sum) / n
    }
    val means = sums.map(_ / n) //各列均值

    //datum是返回函数的参数
    (datum:Vector) => {
      val normalizedArray = (datum.toArray,means,stdevs).zipped.map(
        (value,mean,stdev) =>
          //标准差小于等于0
          if (stdev <= 0) (value -mean) else (value - mean) / stdev
      )
      Vectors.dense(normalizedArray)
    }
  }


  def  clusterTake3(rawData :RDD[String]): Unit ={
    //one-hot解析分类类型的函数
    val  parseFunction = buildCategoricalAndLabelFunction(rawData)
    val data = rawData.map(parseFunction).values
    //标准化
    val normalizedData = data.map(buildNormalizationFuncation(data)).cache()

    (80 to 160 by 10 ).map(k =>
      (k, clusteringScore2(normalizedData,k))).toList.foreach(println)

    normalizedData.cache()
  }

  /**
   * one-hot处理分类 ，返回函数
   * @param rawData
   * @return
   */
  def buildCategoricalAndLabelFunction(rawData:RDD[String]):(String =>(String,Vector))={

    val splitData = rawData.map(_.split(','))
    //zipWithIndex 索引作为value
    val protocols = splitData.map(_(1)).distinct.collect().zipWithIndex.toMap

    val services= splitData.map(_(2)).distinct().collect().zipWithIndex.toMap
    val tcpStates = splitData.map(_(3)).distinct().collect().zipWithIndex.toMap

    (line:String)=>{
      val buffer = line.split(',').toBuffer
      val protocol = buffer.remove(1)
      val service = buffer.remove(1)
      val tcpState = buffer.remove(1)
      val label = buffer.remove(buffer.length -1)
      val vector = buffer.map(_.toDouble)

      val newProtocolFeatures = new Array[Double](protocols.size)
      newProtocolFeatures(protocols(protocol)) = 1.0 //指定索引来更改数组里的值，
      val newServiceFeatures = new Array[Double](services.size)
      newServiceFeatures(protocols(protocol)) =1.0

      val newTcpStateFeatures = new Array[Double](tcpStates.size)
      newTcpStateFeatures(tcpStates(tcpState)) = 1.0

      vector.insertAll(1, newTcpStateFeatures) //在数组的第一个位置插入一个数组
      vector.insertAll(1, newServiceFeatures)
      vector.insertAll(1,newProtocolFeatures)

      (label,Vectors.dense(vector.toArray))
    }
  }

  def  clusterTake4(rawData: RDD[String]): Unit ={

    val parseFunction = buildCategoricalAndLabelFunction(rawData)
    val labelsAndData = rawData.map(parseFunction) //返回 label,vector

    //mapValues 只修改value的值，key不变
    val normalizedLabelsAndData =
      labelsAndData.mapValues(buildNormalizationFuncation(labelsAndData.values)).cache()

    (80 to 160 by 10).map(k=>
      (k,clusteringScore3(normalizedLabelsAndData,k))).toList.foreach(println)

    normalizedLabelsAndData.unpersist()
  }

  /**
   * 使用熵作为同类性度量。 良好的聚类结果簇中样本类别大体相同，因而熵值较低
   * @param normalizedLabelsAndData
   * @param k
   * @return
   */
  def clusteringScore3(normalizedLabelsAndData:RDD[(String,Vector)],k :Int)={

    val kmeans = new KMeans()
    kmeans.setK(k)
    kmeans.setRuns(10)
    kmeans.setEpsilon(1.0e-6)
    val model = kmeans.run(normalizedLabelsAndData.values)
    //开始预测,返回 label，cluster
    val labelsAndClusters = normalizedLabelsAndData.mapValues(model.predict)

    val clustersAndLabels = labelsAndClusters.map(_.swap) //swap交换元组的两个元素label

    //相同的簇包含哪些标签
    // groupByKey将RDD[K,V]中每个K对应的V值，合并到一个集合Iterable[V]中，
    val labelsInCluster = clustersAndLabels.groupByKey().values  //返回Iterable[label]

    //groupBy 根据标签分成多个子集合
    val labelCounts = labelsInCluster.map(_.groupBy(l => l).map(_._2.size))

    val n = normalizedLabelsAndData.count() //总数量

    labelCounts.map(m=> m.sum * entropy(m)).sum / n
  }

  /**
   * 计算熵
   * @param counts
   * @return
   */
  def entropy(counts:Iterable[Int]) ={
    val values = counts.filter(_ > 0)
    val n :Double = values.sum  //一个簇里元素的总数量
    values.map{v =>
      val p = v / n
      -p * math.log(p)
    }.sum

  }


  def anomalies(rawData:RDD[String]): Unit ={

    val parseFunction = buildCategoricalAndLabelFunction(rawData)
    val originalAndData = rawData.map(line => (line,parseFunction(line)._2))
    val data = originalAndData.values
    val normalizeFunction = buildNormalizationFuncation(data)
    //检测异常
    val anomalyDetector = buildAnomalyDetector(data,normalizeFunction)

    //检查原始数据中所有数据，得到异常数据
    val anomalies = originalAndData.filter{
      case (original,datum) => anomalyDetector(datum)
    }.keys

    anomalies.take(10).foreach(println)
  }

  def buildAnomalyDetector(
          data:RDD[Vector],
          normalizedFunction:(Vector=>Vector)):(Vector => Boolean) ={

    val normalizedData = data.map(normalizedFunction).cache()

    val kmeans = new KMeans()
    kmeans.setK(150)
    kmeans.setRuns(10)
    kmeans.setEpsilon(1.0e-6)

    val model = kmeans.run(normalizedData)

    normalizedData.unpersist()

    val distances = normalizedData.map(datum => distToCentroid(datum,model))
    val threshold = distances.top(100).last

    (datum:Vector) => distToCentroid(normalizedFunction(datum),model) > threshold
  }
}
