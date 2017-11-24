package com.sparkstreaming

import java.util.Date

import com.alibaba.fastjson.JSON
import kafka.serializer.StringDecoder
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Minutes, Seconds, StreamingContext}

/**
 * Created by lichangyue on 2016/10/21.
 */
object OrderStreaming {

  val privateKey = "MIIBUwIBADANBgkqhkiG9w0BAQEFAASCAT0wggE5AgEAAkEAjbSzK8dEgcU2PztVyiIB9BjCDgwjpN4g3H15Uq2Dxw12hyKRZDXmRuXTjxaEqfAE4aVTzIoYM0aJLUrpHhwqAwIDAQABAkAgcbf4FQalY57Y+V/aCNFDrwt3JeZfUBBcC0pk2J9sSlwbQ++SN778dvK+yhZ8Q9jrDVVIo9V+iZSpHdqXjKzRAiEA0+iNkWy8CLY+PAsL3iEFU8W7LI9P8RRznXywqu1bNwsCIQCrMMKBGUBZdVL3P9oKAuoF+Nmdjrm3KPCWGZ8CBsvT6QIgRK5V2/FrDEPM7fcClK8NI/atUKbuWQuw4TU9qVievLsCIBY+AJeLc1vsLXpodmjklglumr+o4qJUlGW8MHev8F25AiBe3cgLML5cWgrDCGQhBJNtJalNw9ETzay3pGMRCXYc1Q=="

  case class Order(uid:String ,ct:String, name:String, type1:String,type2:String,ctdate :String)

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR);
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.ERROR);

    val conf = new SparkConf().setAppName("OrderStreaming").setMaster("local")
    val sc = new SparkContext(conf)
    val ssc = new StreamingContext(sc, Seconds(30))
   /* val hiveContext = new HiveContext(sc)
    import hiveContext.implicits._
    hiveContext.setConf("hive.exec.dynamic.partition", "true")
    hiveContext.setConf("hive.exec.dynamic.partition.mode", "nonstrict")
    hiveContext.setConf("hive.exec.max.dynamic.partitions.pernode", "100000")
    hiveContext.setConf("hive.exec.max.dynamic.partitions", "100000")*/

    //    val ssc = new StreamingContext(conf, Seconds(30))
    // Kafka configurations
    val topics = Set("oms_order")
    val brokers = "10.58.188.206:9092,10.58.22.220:9092,10.58.22.221:9092"
    val kafkaParams = Map[String, String](
      "metadata.broker.list" -> brokers, "serializer.class" -> "kafka.serializer.StringEncoder")

    // Create a direct stream
    val kafkaStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topics)


    val events = kafkaStream.flatMap(line => {
      //val data = JSONObject.fromObject(line._2)
      val data = JSON.parseObject(line._2)
      Some(data)
    })

    events.print()

    ssc.start()
    ssc.awaitTermination()
  }

}
