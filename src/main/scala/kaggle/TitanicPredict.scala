package kaggle

import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.stat.{Statistics, MultivariateStatisticalSummary}
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.mllib.tree.DecisionTree

/**
 * Created by dayue on 2016/12/18.
 */
object TitanicPredict {

  def main(args :Array[String]) {


    val conf = new SparkConf().setAppName(this.getClass.getSimpleName).setMaster("local")

    val sc = new SparkContext(conf)

    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._
    import sqlContext.sql
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

      PassengerId,Survived,Pclass,Name,Sex,Age,SibSp,Parch,Ticket,Fare,Cabin,Embarked
     */

    val trainRDD =  sc.textFile("data/kaggle/titanic/train.csv")

    val testRDD =  sc.textFile("data/kaggle/titanic/test.csv")

    val train_df = trainRDD.map{line =>
      val arr = line.split(",",13)
      (arr(0),arr(1).toDouble,arr(2),arr(3),arr(4),arr(5),arr(6),arr(7),arr(8),arr(9),arr(10),arr(11),arr(12))
    }.toDF("PassengerId","Survived","Pclass","FirstName","LastName",
        "Sex","Age","SibSp","Parch","Ticket","Fare","Cabin","Embarked")

    val test_df = testRDD.map{line =>
      val arr = line.split(",",13)
      (arr(0),arr(1).toDouble,arr(2),arr(3),arr(4),arr(5),arr(6),arr(7),arr(8),arr(9),arr(10),arr(11),arr(12))
    }.toDF("PassengerId","Survived","Pclass","FirstName","LastName",
        "Sex","Age","SibSp","Parch","Ticket","Fare","Cabin","Embarked")

    train_df.printSchema()

    //数值型特征的统计量
    train_df.describe("PassengerId","Survived","Pclass","Age","SibSp","Parch","Fare").show()



    //分类特征的分布
    train_df.describe("LastName","Sex","Ticket","Cabin").show()

   // train_df.select("Sex").groupBy("Sex").count.show()


    //train_df[['Pclass', 'Survived']].
    // groupby(['Pclass'], as_index=False).mean().sort_values(by='Survived', ascending=False)


    //pclass  ：我们观察pclass与survived有很大的相关(>0.5).决定把这个特征加到我们的模型中。
    train_df.select("Pclass","Survived").groupBy("Pclass").mean("Survived").show()
/*
    +------+-------------------+
    |Pclass|      avg(Survived)|
    +------+-------------------+
    |     1| 0.6296296296296297|
    |     2|0.47282608695652173|
    |     3|0.24236252545824846|
    +------+-------------------+
*/

    //sex :女性更有可能生存下来
    train_df.select("Sex","Survived").groupBy("Sex").mean("Survived").show()
 /*
    +------+-------------------+
    |   Sex|      avg(Survived)|
    +------+-------------------+
    |female| 0.7420382165605095|
    |  male|0.18890814558058924|
    +------+-------------------+
    */


    //SibSp和Parch：这两个特征和survived是0相关，从这些独立的特征中派生新特征是最好的。
    train_df.select("SibSp","Survived").groupBy("SibSp").mean("Survived").orderBy($"avg(Survived)".desc).show()
/*
    +-----+-------------------+
    |SibSp|      avg(Survived)|
    +-----+-------------------+
    |    1| 0.5358851674641149|
    |    2| 0.4642857142857143|
    |    0|0.34539473684210525|
    |    3|               0.25|
    |    4|0.16666666666666666|
    |    5|                0.0|
    |    8|                0.0|
    +-----+-------------------+
*/

    train_df.select("Parch","Survived").groupBy("Parch").mean("Survived").sort($"avg(Survived)".desc).show()
/*
    +-----+-------------------+
    |Parch|      avg(Survived)|
    +-----+-------------------+
    |    3|                0.6|
    |    1| 0.5508474576271186|
    |    2|                0.5|
    |    0|0.34365781710914456|
    |    5|                0.2|
    |    4|                0.0|
    |    6|                0.0|
    +-----+-------------------+*/


    train_df.select("Embarked","Pclass","Sex","Survived")
      .cube($"Embarked",$"Pclass",$"Sex")
      .agg("Survived"->"avg")
      .orderBy($"Embarked".desc,$"Pclass" ,$"Sex").show()
/*

      +--------+------+------+-------------------+
      |Embarked|Pclass|   Sex|      avg(Survived)|
      +--------+------+------+-------------------+
      |       S|  null|  null|0.33695652173913043|
      |       S|  null|female| 0.6896551724137931|
      |       S|  null|  male| 0.1746031746031746|
      |       S|     1|  null| 0.5826771653543307|
      |       S|     1|female| 0.9583333333333334|
      |       S|     1|  male|0.35443037974683544|
      |       S|     2|  null| 0.4634146341463415|
      |       S|     2|female| 0.9104477611940298|
      |       S|     2|  male|0.15463917525773196|
      |       S|     3|  null|0.18980169971671387|
      |       S|     3|female|              0.375|
      |       S|     3|  male|0.12830188679245283|
      |       Q|  null|  null|0.38961038961038963|
      |       Q|  null|female|               0.75|
      |       Q|  null|  male|0.07317073170731707|
      |       Q|     1|  null|                0.5|
      |       Q|     1|female|                1.0|
      |       Q|     1|  male|                0.0|
      |       Q|     2|  null| 0.6666666666666666|
      |       Q|     2|female|                1.0|
      +--------+------+------+-------------------+
*/


    //删除
    val clean_train_df = train_df.drop("Ticket").drop("Cabin")
    val clean_test_df = test_df.drop("Ticket").drop("Cabin")



    val title_train_DF = clean_train_df.select("PassengerId","LastName").map{row =>

      val set = Set("Lady", "the Countess","Capt", "Col",
        "Don", "Dr", "Major", "Rev", "Sir", "Jonkheer", "Dona")

      var title =  row.getAs[String]("LastName").split("\\.",2)(0).trim
        if(set.contains(title)){
          title ="Rare"
        } else if( title =="Ms"||title == "Mlle"){
          title = "Miss"
        } else if( title =="Mme"){
          title = "Mrs"
        }
        (row.getString(0) ,title)

    }.toDF("PassengerId","Title")


    val train_df1 = clean_train_df.join(title_train_DF ,Seq("PassengerId")).drop("LastName").drop("FirstName")

    train_df1.groupBy("Title","Sex").count().show(10)
/*
    +-------------+------+-----+
    |        Title|   Sex|count|
    +-------------+------+-----+
    |         Miss|female|  182|
    |           Ms|female|    1|
    |          Mme|female|    1|
    |       Master|  male|   40|
    | the Countess|female|    1|
      |           Dr|female|    1|
      |          Mrs|female|  125|
      |           Mr|  male|  517|
      |         Capt|  male|    1|
      |         Lady|female|    1|
      +-------------+------+-----+
*/

    train_df1.select("Title","Survived").groupBy("Title").avg("Survived").show(10)
/*

    | Title|      avg(Survived)|
      +------+-------------------+
    |Master|              0.575|
      |  Rare|0.34782608695652173|
      |   Mrs| 0.7936507936507936|
      |    Mr|0.15667311411992263|
      |  Miss| 0.7027027027027027|
      +------+-------------------+
*/

    val title_df = train_df1.select("PassengerId","Title","Sex").map{row =>

      val sexes = Map("female"-> 1, "male"->0)
      val title_mapping = Map ("Mr"-> 1, "Miss"-> 2, "Mrs"-> 3, "Master"-> 4, "Rare"-> 5)
      val title = row.getAs[String]("Title")

      (row.getString(0) ,title_mapping.getOrElse(title,0),sexes.getOrElse(row.getString(2),0))
    }.toDF("PassengerId","Title","Sex")

    val train_df2 = clean_train_df.drop("Sex").join(title_df ,Seq("PassengerId")).drop("LastName").drop("FirstName")


    train_df2.show(10)

    /**
    补全连续的数值特征

    猜测缺失值更精确的方式是使用其他相关的特征。在我们的案例中，我们注意到age，gender，pclass之间存在相关性，
    使用通过pclass和gender特征结合的集合的中间值来猜年龄值。因此，pclass=1和gender=0的中间年龄，等等。
     */


    val age = train_df2.select("Age","Pclass","Sex")

    val Pclass1_Sex0 = age.filter("Pclass = '1' and Sex = 0 and Age  != '' ").select("Age").map(
      row =>row.getString(0).toLong).toDF("Age")

    Pclass1_Sex0.registerTempTable("Pclass1_Sex0")

    sql("SELECT PERCENTILE(Age, 0.5) FROM Pclass1_Sex0").show()




  }

  def toDouble(s :String) ={
    if ("".equals(s)) Double.NaN  else  s.toDouble
  }


}
