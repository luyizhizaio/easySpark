package com.analysis

import org.apache.spark.mllib.recommendation.{ALS, Rating}
import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by lichangyue on 2017/3/23.
 */
object RunRecommenderTest {

  def main(args: Array[String]) {

    val conf = new SparkConf()
      .setAppName(this.getClass.getSimpleName).setMaster("local")

    val sc = new SparkContext(conf)

//    1000002 1000006 33  #用户ID，艺术家ID,播放次数
    val rawUserArtistData = sc.textFile("data/analy/user_artist_data.txt")

    rawUserArtistData.map(_.split(" ")(0).toDouble).stats() //返回统计描述

//    1134999	06Crazy Life   artistid artist_name
    val rawArtistData = sc.textFile("data/analy/artist_data.txt")

    //flatMap将每个输入对应的零个或多个结果组成的集合简单展开，然后放入到一个更大的RDD 中
    val artistByID = rawArtistData.flatMap{line =>
      val (id,name ) = line.span(_ !='\t') //第一个空隙是不是制表符
      if(name.isEmpty){
        None
      }else {
        try{
          Some((id.toInt,name.trim))
        }catch{

          case e :NumberFormatException => None
        }
      }
    }
    artistByID.take(10).foreach(println)

//    1092764 1000311; badid, goodid
    val rawArtitstAlias = sc.textFile("data/analy/artist_alias.txt")


    //由于某种原因有些行没有艺术家 的第一个ID
    val artistAlias = rawArtitstAlias.flatMap{line =>
      val tokens = line.split('\t')
      if(tokens(0).isEmpty){
        None
      } else{
        Some((tokens(0).toInt , tokens(1).toInt))
      }
    }.collectAsMap()  //转成一个Map

    //从包含艺术家名字的RDD中查找名称
    println(artistByID.lookup(10299751).head)
//    Ki-ya-Kiss

    //构建模型
    val bArtistAlias = sc.broadcast(artistAlias)

    val trainData = rawUserArtistData.map{line =>

      val Array(userID , artistID,count) = line.split(' ').map(_.toInt)

      val finalArtistID = bArtistAlias.value.getOrElse(artistID,artistID)

      Rating(userID , finalArtistID , count)
    }.cache()


    val model = ALS.trainImplicit(trainData,10,5 ,0.01,1.0)


    println(model.userFeatures.mapValues(_.mkString(",")).first())
    /*
    (1000002,0.4488735496997833,0.029437409713864326,
    -0.2624022364616394,0.409451425075531,0.18170498311519623,
    0.22908072173595428,0.20915783941745758,0.12869581580162048,
    0.6088367700576782,-0.2112993448972702)
*/
    //检查推荐结果

    val rawArtistsForUser = rawUserArtistData.map(_.split(' '))
      .filter{case Array(user,_,_) =>user.toInt == 2093760}

    //取出2093760关联的艺术家
    val existingProducts = rawArtistsForUser.map{
      case Array(_,artist,_) => artist.toInt
    }.collect.toSet

    //过滤艺术家并打印
    artistByID.filter{case (id,name) =>
      existingProducts.contains(id)
    }.values.collect().foreach(println)



    val recomendations = model.recommendProducts(2093760,5)

    /*返回的结果:用户ID,艺术家ID,和rating它是一个在0 到1 之间的模糊值，
    值越大，推荐质量越好。它不是概率，但可以把它理解成对0/1 值的一个估计，0 表示用
      户不喜欢播放艺术家的歌曲，1 表示喜欢播放艺术家的歌曲。
    */
    recomendations.foreach(println)


    /*
    5.查找艺术家的名字
     */
    //艺术家ID 集合
    val recommendedProductIDs = recomendations.map(_.product).toSet

    artistByID.filter{case (id,name)=>
      recommendedProductIDs.contains(id)
    }.values.collect().foreach(println)


    /**
     *6.评价推荐质量
     * AUC:推荐引擎，为每个用户计算AUC 并取其平均值，最后的结果指标稍有不同，可称为“平均AUC”。
     * ，我们采用AUC 作为一种普遍和综合的测量整体模型输出质量的手段
     */









  }

}
