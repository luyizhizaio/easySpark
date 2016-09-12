package com.scala

import java.util.Date


/**
 * Created by lichangyue on 2016/9/8.
 */
object FuncPian {


  def main(args: Array[String]) {

    val date = new Date()
    val logwithDateBound =log(date,_:String)
    logwithDateBound("message1")
    logwithDateBound("message2")
    logwithDateBound("message3")



  }

  def log(date:Date,message:String)={

    println(date +"--------"+ message)
  }



}
