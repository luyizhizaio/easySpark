package com.mllib

import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.linalg.distributed.{IndexedRowMatrix, IndexedRow}
import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by lichangyue on 2017/1/20.
 */
object BlockMatrixDemo extends App {


  val conf = new SparkConf().setAppName(this.getClass.getSimpleName).setMaster("local")
  val sc = new SparkContext(conf)

  //隐式转换函数
  implicit def double2long(x:Double)=x.toLong

  val rdd1 = sc.parallelize(
  Array(
    Array(1.0,1.0,2.0,2.0),
    Array(1.0,1.0,2.0,2.0),
    Array(3.0,3.0,4.0,4.0),
    Array(3.0,3.0,4.0,4.0)
  )).map(f => IndexedRow(f.take(1)(0),Vectors.dense(f)))

  rdd1.foreach(x=> println(x.vector.toJson))


  val indexRowMatrix = new IndexedRowMatrix(rdd1)
//  IndexedRowMatrix转换成BlockMatrix，指定每块的行列数
  val blockMatrix = indexRowMatrix.toBlockMatrix(2,2)

  blockMatrix.blocks.foreach(f=>println("Index:"+f._1+"MatrixContent:"+f._2))
//  Index:(0,0)MatrixContent:2 x 2 CSCMatrix
//  (1,0) 2.0
//  (1,1) 2.0
//  Index:(1,1)MatrixContent:2 x 2 CSCMatrix
//  (1,0) 8.0
//  (1,1) 8.0
//  Index:(1,0)MatrixContent:2 x 2 CSCMatrix
//  (1,0) 6.0
//  (1,1) 6.0
//  Index:(0,1)MatrixContent:2 x 2 CSCMatrix
//  (1,0) 4.0
//  (1,1) 4.0

  val localMat = blockMatrix.toLocalMatrix()

  println(localMat)
//  0.0  0.0  0.0  0.0
//  2.0  2.0  4.0  4.0
//  0.0  0.0  0.0  0.0
//  6.0  6.0  8.0  8.0

  //转换成CoordinateMatrix
  blockMatrix.toCoordinateMatrix()

  //转换成IndexedRowMatrix
  blockMatrix.toIndexedRowMatrix()

  //验证分块矩阵的合法性
  blockMatrix.validate()

}
