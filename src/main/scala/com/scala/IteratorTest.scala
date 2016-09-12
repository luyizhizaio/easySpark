package com.scala

/**
 * Created by lichangyue on 2016/9/8.
 */
object IteratorTest {

  def main(args: Array[String]) {

    val list = Array.range(10 ,20)
    val it =list.iterator

    println("min: "+it.min)
    println("max: " +it.max)

  }

}
