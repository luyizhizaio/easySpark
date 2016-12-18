package kaggle

import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.mllib.tree.DecisionTree

/**
 * Created by dayue on 2016/12/18.
 */
object TitanicPredict {

//  PassengerId,Survived,Pclass,Name,Sex,Age,SibSp,Parch,Ticket,Fare,Cabin,Embarked
  case class Passenger(PassengerId:String,
                      Survived:String,
                      Pclass:String,
                      Sex:String,
                      Age:String,
                      SibSp:String,
                      Parch:String,
                      Fare:String,
                      Embarked:String)

  def main(args :Array[String]) {


    val conf = new SparkConf().setAppName(this.getClass.getSimpleName).setMaster("local")

    val sc = new SparkContext(conf)

    /*val hiveContext = new HiveContext(sc)*/

    /**
    VARIABLE DESCRIPTIONS:
    survival        Survival
                    (0 = No; 1 = Yes)
    pclass          Passenger Class 乘客等级
                    (1 = 1st; 2 = 2nd; 3 = 3rd)
    name            Name
    sex             Sex
    age             Age
    sibsp           Number of Siblings/Spouses Aboard 兄弟姐妹/配偶的数量
    parch           Number of Parents/Children Aboard 父母/孩子的数量
    ticket          Ticket Number
    fare            Passenger Fare 乘客票价
    cabin           Cabin 舱
    embarked        Port of Embarkation 港口
                    (C = Cherbourg; Q = Queenstown; S = Southampton)
     */

    val trainRDD =  sc.textFile("file/kaggle/titanic/train.csv")

    val testRDD =  sc.textFile("file/kaggle/titanic/test.csv")

    trainRDD.take(10).foreach(println(_))
//    testRDD.toDF

    val train = trainRDD.filter{case line => !line.startsWith("PassengerId") && line.split(",").size ==13}
      .map(line =>{
        println(line)
        var arr= line.split(",")
        //  PassengerId,Survived,Pclass,Name,Sex,Age,SibSp,Parch,Ticket,Fare,Cabin,Embarked
        //        (PassengerId:Int,
        //          Survived:Int,
        //        Pclass:Int,
        //        Sex:Int,
        //        Age:Int,
        //        SibSp:Int,
        //        Parch:Int,
        //        Fare:Int,
        //        Embarked:String)
        //        1,0,3,"Braund, Mr. Owen Harris",male,22,1,0,A/5 21171,7.25,,S

        var sex = if ("male".equals(arr(5))) "1" else "2"  //male =1 ,female =2
        var Embarked = arr(12) match{
            case "C" => "1"
            case "Q" => "2"
            case "S" => "3"
            case _ :String => "1"
          }  //(C = Cherbourg; Q = Queenstown; S = Southampton)
        println(arr(2),sex,arr(6),arr(7),arr(8),arr(10),Embarked)
        (arr(1).toDouble,Array(arr(2),sex,arr(6),arr(7),arr(8),arr(10),Embarked))

      }).filter{case line => ! line._2.contains("")}.map(line=> {
      LabeledPoint(line._1.toDouble,
        Vectors.dense(line._2.map(_.toDouble)))
    })

    /**
     * PassengerId,Pclass,Name,Sex,Age,SibSp,Parch,Ticket,Fare,Cabin,Embarked
     * 892,3,"Kelly, Mr. James",male,34.5,0,0,330911,7.8292,,Q
     */

    val test = testRDD.filter{case line => !line.startsWith("PassengerId") && line.split(",").size ==12}
      .map(line => {
        var arr = line.split(",")
        var sex = if(arr(4) == "male") "1" else "2"  //male =1 ,female =2
        var Embarked = arr(11) match{
            case "C" => "1"
            case "Q" => "2"
            case "S" => "3"
            case _ => "1"
          }  //(C = Cherbourg; Q = Queenstown; S = Southampton)

        Array(arr(1),sex,arr(5),arr(6),arr(7),arr(9),Embarked)
      }).filter{case line => ! line.contains("")}.map(line=> {
        Vectors.dense(line.map(_.toDouble))
    })

    val numClasses =2
    val categoricalFeaturesInfo = Map[Int,Int]()
    val impurity ="gini"
    //最大深度
    val maxDepth = 5
    //最大分值
    val maxBins =32
    //模型训练
    val model = DecisionTree.trainClassifier(train,numClasses,categoricalFeaturesInfo,
      impurity,maxDepth,maxBins)

    //模型预测
    val labeAndPreds = test.map{
      point =>
        val prediction = model.predict(point)
        prediction
    }
    //测试值与真实值对比
    val print_predict = labeAndPreds.take(15)
    println("prediction")

    for(i <- print_predict){
      println(i)
    }

    /*//树的错误率
    val testErr = labeAndPreds.filter(r => r._1 !=r._2).count.toDouble/testData.count()
    println("test error:" + testErr)
    //树的判断值
    println("learned classfifcation tree model"+model.toDebugString)*/

    /**
    //转换成向量
    val tree1 = data1.map(line => {
      val parts = line.split(",")
      LabeledPoint(parts(0).toDouble,Vectors.dense(parts(1).split(" ").map(_.toDouble)))

    })


    val tree2 = data2.map(line =>{
      val parts = line.split(",")
      LabeledPoint(parts(0).toDouble, Vectors.dense(parts(1).split(" ").map(_.toDouble)))

    })
    //赋值
    val (trainData,testData) = (tree1,tree2)

    //分类数量
    val numClasses =2
    val categoricalFeaturesInfo = Map[Int,Int]()
    val impurity ="gini"
    //最大深度
    val maxDepth = 5
    //最大分值
    val maxBins =32
    //模型训练
    val model = DecisionTree.trainClassifier(trainData,numClasses,categoricalFeaturesInfo,
      impurity,maxDepth,maxBins)

    //模型预测
    val labeAndPreds = testData.map{
      point =>
        val prediction = model.predict(point.features)
        (point.label, prediction)
    }
    //测试值与真实值对比
    val print_predict = labeAndPreds.take(15)
    println("label"+"\t"+"prediction")

    for(i <- print_predict){
      println(i._1 +"\t"+ i._2)
    }

    //树的错误率
    val testErr = labeAndPreds.filter(r => r._1 !=r._2).count.toDouble/testData.count()
    println("test error:" + testErr)
    //树的判断值
    println("learned classfifcation tree model"+model.toDebugString)

  }



     */

  }

}
