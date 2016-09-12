package com.ml

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.linalg.{Matrices, Vector, Vectors}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.util.MLUtils

/**
 * Created by lichangyue on 2016/8/26.
 */
object MLDataType {

  def main(args: Array[String]) {

    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR);
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.ERROR);

    //1.dense vector Create a dense vector (1.0, 0.0, 3.0).
    val dv = Vectors.dense(1.0,0.0,3.0)
    dv.toArray.foreach(println(_))
    //1.2.sparse vector
    val sv1 = Vectors.sparse(3,Array(0,2),Array(1.0,3.0))
    sv1.toArray.foreach(println(_))

    val sv2 = Vectors.sparse(3,Seq((0,1.0),(2,3.0)))
    sv2.toArray.foreach(println(_))


    //2.Labeled point, 一个标签,一个特征向量

    val pos = LabeledPoint(1.0,Vectors.dense(1.0,0.0,3.0))

    val neg = LabeledPoint(0.0,Vectors.sparse(3, Array(0,2),Array(1.0,3.0)))
    neg.features.toArray.foreach(print ) //获取向量
    println(neg.label) //标签

    //从文本中读取标签向量

   /* val sc = new SparkContext(new SparkConf().setMaster("local").setAppName("TEST"))
    MLUtils.loadLibSVMFile(sc, "data/mllib/sample_libsvm_data.txt")*/


    //3.本地矩阵, 密集矩阵，3行2列，((1.0, 2.0), (3.0, 4.0), (5.0, 6.0))

    val dm = Matrices.dense(3,2,Array(1.0,3.0,5.0,2.0,4.0,6.0))

    //稀疏矩阵 ,((9.0, 0.0), (0.0, 8.0), (0.0, 6.0))
    //对应新列的开始索引，行索引，非0的值
    val sm = Matrices.sparse(3,2,Array(0,1,3),Array(0,2,1),Array(9,6,8))
    println("row num:" + sm.numRows)
    println(sm.numCols)

























  }

}
