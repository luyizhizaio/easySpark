package com.sparkstreaming

/**
 * Created by lichangyue on 2017/2/14.
 */
import com.alibaba.fastjson.JSONObject
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by lichangyue on 2017/2/14.
 */
object SendtoKafka extends  App {

  Logger.getLogger("org.apache.spark").setLevel(Level.ERROR);
  Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.ERROR);

  val topic = "ufpdPortrait"
  val brokerList ="10.58.188.206:9092,10.58.22.220:9092,10.58.22.221:9092"

  val conf = new SparkConf().setAppName(this.getClass.getSimpleName).setMaster("local")
  val sc = new SparkContext(conf)




  val kafkaProducer = new KafkaProducer()
  kafkaProducer.setBrokerList(brokerList)
  kafkaProducer.setProducerType("sync")
  kafkaProducer.init()


  val attr = sc.textFile("hdfs://S1SA300:8020/user/hive/warehouse/stat.db/ufpd_account_attr_num_send_to_kafka/ctdate=20170213").map{line=>
    val arr = line.split("\u0001")
    (arr(0),arr(1),arr(2),arr(3),arr(4),arr(5),arr(6))
  }

  attr.foreach(row =>{
    val ufpd = row._1
    val lognum = row._2.toInt
    val regNum = row._3.toInt
    val accountNum = row._4.toInt
    val mobileNum = row._5.toInt
    val ipNum = row._6.toInt
    val addressNum = row._7.toInt

    if(lognum != 0){
      send2Kafka(ufpd ,"fpRelLoginUserInAll",lognum)
    }
    if(regNum != 0){
      send2Kafka(ufpd ,"fpRelRegUserInAll",regNum)
    }
    if(accountNum != 0){
      send2Kafka(ufpd ,"fpRelUserInAll",accountNum)
    }
    if(mobileNum != 0){
      send2Kafka(ufpd ,"fpRelMobileInAll",mobileNum)
    }
    if(ipNum != 0){
      send2Kafka(ufpd ,"fpRelIpInAll",ipNum)
    }
    if(addressNum != 0){
      send2Kafka(ufpd ,"fpRelAddressInAll",addressNum)
    }
    println("send " + ufpd + " message finish")
  })


  def send2Kafka(ufpd: String, columnName:String ,columnValue:Int): Unit ={

    val json = new JSONObject();
    json.put("ufpd", ufpd);
    json.put("columnName", columnName);
    json.put("columnValue", columnValue);

    try{
      //发送消息
      kafkaProducer.send(topic,json)
    }catch{
      case ex:Exception => ex.printStackTrace()
    }

  }


  kafkaProducer.shutdown()
  sc.stop()

}