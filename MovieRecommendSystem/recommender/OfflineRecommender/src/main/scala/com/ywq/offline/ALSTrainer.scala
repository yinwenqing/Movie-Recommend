package com.ywq.offline

import breeze.numerics.sqrt
import com.atguigu.offline.OfflineRecommender
import org.apache.spark.SparkConf
import org.apache.spark.mllib.recommendation.{ALS, Rating}
import org.apache.spark.mllib.recommendation.MatrixFactorizationModel
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

case class MovieRating(val uid: Int, val mid: Int, val score: Double, val timestamp: Int)

/**
  * MongoDB的连接配置
  *
  * @param uri MongoDB的连接
  * @param db  MongoDB要操作数据库
  */
case class MongoConfig(val uri: String, val db: String)

object ALSTrainer {
  def main(args: Array[String]): Unit = {

    val config = Map(
      "spark.cores" -> "local[*]",
      "mongo.uri" -> "mongodb://192.168.43.31:27017/recommender",
      "mongo.db" -> "reommender"
    )

    val MONGODB_RATING_COLLECTION = "Rating"

    //创建SparkConf
    val sparkConf = new SparkConf().setAppName("ALSTrainer").setMaster(config("spark.cores"))

    //创建SparkSession
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()

    val mongoConfig = MongoConfig(config("mongo.uri"), config("mongo.db"))

    import spark.implicits._

    //加载评分数据
    val RatingRDD = spark
      .read
      .option("uri", mongoConfig.uri)
      .option("collection", MONGODB_RATING_COLLECTION)
      .format("com.mongodb.spark.sql")
      .load()
      .as[MovieRating]
      .rdd
      .map(rating => Rating(rating.uid, rating.mid, rating.score)).cache()

    //输出最优参数
    adjustALSParams(RatingRDD)

    //关闭Spark
    spark.close()

  }

  //  输出最终的最优参数
  def adjustALSParams(trainData: RDD[Rating]): Unit = {
    val result = for (rank <- Array(30, 40, 50, 60, 70); lambda <- Array(1, 0.1, 0.001))
      yield {
        val model = ALS.train(trainData, rank, 5, lambda)
        val rmse = getRmse(model, trainData)
        (rank, lambda, rmse)
      }
    println(result.sortBy(_._3).head)
    println()
    println()
    println()
    println()
  }

  def getRmse(model: MatrixFactorizationModel, trainData: RDD[Rating]): Double = {
    //需要构造一个usersProducts RDD[(Int,Int)]
    val userMovies = trainData.map(item => (item.user, item.product))
    val predictRating = model.predict(userMovies)

    //转换形式
    val real = trainData.map(item => ((item.user, item.product), item.rating))
    val predict = predictRating.map(item => ((item.user, item.product), item.rating))

    //均方误差
    sqrt(
      real.join(predict).map { case ((uid, mid), (real, pre)) =>
        //真实值和预测值之间的两个差值
        val err = real - pre
        err * err
      }.mean()
    )
  }


}
