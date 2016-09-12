package com.scala

/**
 * Created by lichangyue on 2016/9/8.
 */
object ScalaLearning {


  def main(args: Array[String]) {


    var a:Int = 0


    val numList = List(1,2,3,4,5,6,7,8,9,10)

    val reVal = for{ a <- numList

      if a !=3 ;if a <8
    } yield a

    //输出返回值
    for(b <- reVal){
      println("value of b: " + b)
    }

  }



  def max(x:Int , y:Int):Int ={
    if(x > y ) x  else y
  }

  def  greet(s:String):Unit={
    println(s)
  }



}
