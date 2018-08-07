package com.scala

/**
 * Created by dayue on 2017/5/8.
 */
object ListTest {

  def main(args: Array[String]) {
    val squares =List.tabulate(6)(n=> n * n)

    println( squares)

    val mul = List.tabulate(4,5)(_ * _)
    println(mul)



    val list:List[Int] = List()
    list.::(10)

  }

  for (x <- 1 to 10){
    println(x)
  }


}
