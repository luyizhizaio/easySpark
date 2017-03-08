package com.mllib.feature

import org.apache.spark.ml.feature.VectorIndexer
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



    val data = sqlContext.read.format("libsvm").load("data/mllib/sample_libsvm_data.txt")

    val indexer = new VectorIndexer()
    .setInputCol("features")
    .setOutputCol("indexed")
    .setMaxCategories(10)

    val indexerModel = indexer.fit(data)

    val categoricalFeatures:Set[Int] = indexerModel.categoryMaps.keys.toSet

    println(s"Chose ${categoricalFeatures.size} categorical features:"+categoricalFeatures.mkString(", "))

    // Create new column "indexed" with categorical values transformed to indices
    val indexedData = indexerModel.transform(data)

    indexedData.show()

  }

}
