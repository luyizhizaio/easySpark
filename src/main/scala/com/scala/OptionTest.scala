package com.scala

/**
 * Created by dayue on 2017/5/8.
 */
object OptionTest {

  def main(args: Array[String]) {

    val sites = Map("fff"->"www.fff.com","google"->"bbb")
    println( sites.get("fff"))
    println(show(sites.get("dddd")))


    val a:Option[Int] =Some(5)
    val b:Option[Int] = None
    println(a.getOrElse(10))
    println(b.getOrElse(15))
    println("a is empty:"+a.isEmpty + "a :"+a.get)
    println("b is empty" +b.isEmpty)


    println( a.filter(p => true))
  }

  def  show(x:Option[String]) = x match{
    case Some(s) =>s
    case None => "?"
  }






}
