package com.scala

/**
 * Created by lichangyue on 2016/9/8.
 */
class Marker private (val color:String) {


  println("创建" + this)
  override def toString():String = "color sign:" + color
}

//伴生对象
object Marker{

  private val markers:Map[String,Marker] =Map(
  "red" -> new Marker("red1"),
  "blue" -> new Marker("blue1"),
  "green" -> new Marker("green1")
  )

  def apply(color:String) ={
    if(markers.contains(color)) markers(color) else null
  }

  def getMarker(color :String)={
    if(markers.contains(color)) markers(color) else null
  }

  def main (args: Array[String]) {

    println(Marker("red"))
    // 单例函数调用，省略了.(点)符号
    println(Marker getMarker "blue")
  }
}