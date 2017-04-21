package com.scala

/**
 * Created by lichangyue on 2017/2/10.
 */
object ImplicitTest {

  implicit class IntWithTimes(x:Int){
    def times[A](f: =>A):Unit = {
      def loop(current:Int):Unit=
        if(current > 0){
          f
          loop(current -1)
        }
      loop(x)
    }
  }

}


object ImplicitTest2 extends App {

  import ImplicitTest._

  5 times println("HI")


}


