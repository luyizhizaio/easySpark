package com.mllib

import org.apache.spark.mllib.fpm.FPGrowth
import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by lichangyue on 2017/1/22.
 */
object FPGrowth {

  def main(args: Array[String]) {

    val conf=new SparkConf().setAppName("regression").setMaster("local[1]")
    val sc=new SparkContext(conf)
    val data = sc.textFile("data/mllib/sample_fpgrowth.txt")
    val transactions = data.map(s=> s.trim.split(" "))

    val fpg = new FPGrowth().setMinSupport(0.2).setNumPartitions(10)

    val model = fpg.run(transactions)

    model.freqItemsets.collect().foreach{itemset =>
      println(itemset.items.mkString("[",",","]") + "," + itemset.freq)
    }

    val minConfidence = 0.8

    model.generateAssociationRules(minConfidence).collect().foreach{rule=>
      println(
        rule.antecedent.mkString("[",",","]")
          + " => " +rule.consequent.mkString("[",",","]")
          +"," + rule.confidence

      )

    }


  }

}
