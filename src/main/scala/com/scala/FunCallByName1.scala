package com.scala

/**
 * Created by lichangyue on 2016/9/8.
 */
object FunCallByName1 {

  def main(args: Array[String]) {
    delayed(time())
  }


  def time() ={

    println("获得时间，单位为纳秒")
    System.nanoTime()
  }

  def delayed(t: => Long)={
    println("在delayed 方法内")
    println("参数：" + t)
  }

}
