package com.mllib

import org.apache.spark.mllib.linalg.distributed.{IndexedRowMatrix, IndexedRow, RowMatrix}
import org.apache.spark.mllib.linalg.{Vectors, Matrices}
import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by lichangyue on 2017/1/20.
 */
object MatrixTest {

  def main(args: Array[String]) {

    val conf  = new SparkConf().setMaster("local").setAppName(this.getClass.getSimpleName)
    val sc = new SparkContext(conf)

    val dm = Matrices.dense(3,2,Array(1.0,3.0,1.0,3.0,4.0,5.))

    println(dm)
//    1.0  3.0
//    3.0  4.0
//    1.0  5.0

    val sm = Matrices.sparse(3,2,Array(0,1,3),Array(0,2,1),Array(9,6,8))

    println(sm)
//    3 x 2 CSCMatrix
//    (0,0) 9.0
//    (2,1) 6.0
//    (1,1) 8.0
  val sparseMatrix= Matrices.sparse(3, 3, Array(0, 2, 3, 6), Array(0, 2, 1, 0, 1, 2), Array(1.0, 2.0, 3.0, 4.0, 5.0, 6.0))

  println(sparseMatrix)
//    3 x 3 CSCMatrix
//    (0,0) 1.0
//    (2,0) 2.0
//    (1,1) 3.0
//    (0,2) 4.0
//    (1,2) 5.0
//    (2,2) 6.0


    val rdd = sc.textFile("data/mllib/matrix.txt").map{_.split(" ").map(_.toDouble)}
    .map{line => Vectors.dense(line)}

    val rowMat = new RowMatrix(rdd)

    val summary = rowMat.computeColumnSummaryStatistics()

    println ("mean:"+summary.mean)
    println("方差："+summary.variance)
//    mean:[2.5,3.5,4.5]
//    方差：[4.5,4.5,4.5]


    val rdd2 = sc.textFile("data/mllib/matrix.txt").map{_.split(" ").map(_.toDouble)}
      .map{line => Vectors.dense(line)}
      .map((vd) => new IndexedRow(vd.size,vd))

    rdd2.foreach(println)














  }

}
