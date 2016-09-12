package com.scala


/**
 * Created by lichangyue on 2016/9/8.
 */
object ArrayTest {


  def main(args: Array[String]) {
    //创建二维数组
    var myMatrix = Array.ofDim[Int](3,3)

    //创建矩阵
    for(i <- 0 to 2){
      for(j <- 0 to 2){
        myMatrix(i)(j) = i + j
      }
    }
    //打印二维矩阵
    for(i <- 0 to 2){
      for(j <- 0 to 2){
        print (" "+ myMatrix(i)(j))
      }
      println()
    }


    val myList1 = Array(1.9,2.9,3.4,3.5)
    val myList2 = Array(8.9,7.9,0.4,1.5)
    val myList3 = Array.concat(myList1,myList2,myList1)

    for(x <- myList3){
      println(x)
    }



    //range
    var myArray1 = Array.range(10,20,2)
    var myArray2 = Array.range(10,20)

    for(x <- myArray1){
      print(" " + x)
    }

    println()

    for(x <- myArray2){
      print(" " + x)
    }
  }
}
