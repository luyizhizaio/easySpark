package com.scala

/**
 * Created by dayue on 2017/5/8.
 */
class AppTestBansheng private {


  private val key="something.key"


  private def update():Unit = {
    println("update method")
  }



}

object AppTestBansheng{

  private val key2 = "key2"

//  def  insert
  def apply():AppTestBansheng={
    new AppTestBansheng()
}



}
