package com.mllib.feature

import org.apache.spark.ml.feature.VectorIndexer
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by lichangyue on 2017/3/8.
 */
object VectorIndexerTest {


  def main(args: Array[String]) {


    val conf = new SparkConf().setAppName(this.getClass.getSimpleName).setMaster("local")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)



    //val data = sqlContext.read.format("libsvm").load("data/mllib/sample_libsvm_data.txt")

    val data = sqlContext.createDataFrame(Seq(
      (1,Vectors.dense(1,0.2,2)),
      (0,Vectors.dense(2,0.2,22)),
      (0,Vectors.dense(1,0.4,21)),
      (1,Vectors.dense(1,0.5,52)),
      (1,Vectors.dense(1,0.3,32)),
      (1,Vectors.dense(1,0.7,1))
    )).toDF("label","features")


    val indexer = new VectorIndexer()
    .setInputCol("features")
    .setOutputCol("indexed")
    .setMaxCategories(4) //超过4就当做连续型数值

    val indexerModel = indexer.fit(data)

    val categoricalFeatures:Set[Int] = indexerModel.categoryMaps.keys.toSet

    println(s"Chose ${categoricalFeatures.size} categorical features:"+categoricalFeatures.mkString(", "))

    // Create new column "indexed" with categorical values transformed to indices
    val indexedData = indexerModel.transform(data)

    indexedData.show()

//    +-----+--------------+--------------+
//    |label|      features|       indexed|
//    +-----+--------------+--------------+
//    |    1| [1.0,0.2,2.0]| [0.0,0.2,2.0]|
//    |    0|[2.0,0.2,22.0]|[1.0,0.2,22.0]|
//    |    0|[1.0,0.4,21.0]|[0.0,0.4,21.0]|
//    |    1|[1.0,0.5,52.0]|[0.0,0.5,52.0]|
//    |    1|[1.0,0.3,32.0]|[0.0,0.3,32.0]|
//    |    1| [1.0,0.7,1.0]| [0.0,0.7,1.0]|
//    +-----+--------------+--------------+

  }

}
