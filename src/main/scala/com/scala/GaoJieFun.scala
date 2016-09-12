package com.scala

/**
  * Created by lichangyue on 2016/9/8.
  */
object GaoJieFun {

   def main(args: Array[String]) {


    println(apply(layout,10))


   }

  def apply(f: Int => String, v: Int) =f(v)

  def layout[A](x:A) = "["+  x.toString +"]"


 }
