package com.scala

/**
 * Created by lichangyue on 2016/9/8.
 */
object FuncCurrying {

  def main(args: Array[String]) {


    val str1:String ="Hello "
    val str2:String = "Scala"
    println("str1 + str2= " + strcat(str1)(str2))

  }

  def strcat(s1:String)(s2:String) ={

    s1 + s2
  }
}
