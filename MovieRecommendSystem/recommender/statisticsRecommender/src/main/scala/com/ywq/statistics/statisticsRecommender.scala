package com.ywq.statistics

import java.sql.Date
import java.text.SimpleDateFormat

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

case class Movie(val mid: Int, val name: String, val descri: String, val timelong: String, val issue: String,
                 val shoot: String, val language: String, val genres: String, val actors: String, val directors: String)

/**
  * rating数据集，用户对于电影的评分数据集，用“，”分割
  * 1,                            用户的ID
  * 1029,                         电影的ID
  * 3.0,                          用户对于电影的评分
  * 1260759179                    用户对于电影评分的时间
  */

case class Rating(val uid: Int, val mid: Int, val score: Double, val timestamp: Int)

/**
  * tag数据集，用户对于电影的标签数据集，用“，”分割
  * 15,                           用户的ID
  * 7478,                         电影的ID
  * Cambodia,                     标签的具体内容
  * 1170560997                    用户对电影打标签的时间
  */

case class MongoConfig(val uri: String, val db: String)

object statisticsRecommender {
  val MONGODB_RATING_COLLECTION = "Rating"
  val MONGODB_MOVIE_COLLECTION = "Movie"

  //统计的表的名称
  val RATE_MORE_MOVIES = "RateMoreMovies"
  val RATA_MORE_RECENTLY_MOVIES = "RateMoreRecentlyMovies"
  val AVERAGE_MOVIES = "AverageMovies"
  val GENRES_TOP_MOVIES = "GenresTopMovies"

  //入口方法
  def main(args: Array[String]): Unit = {
    val config = Map(
      "spark.cores" -> "local[*]",
      "mongo.uri" -> "mongodb://linux:27017/recommender",
      "mongo.db" -> "recommender"
    )

    //创建SparkConf配置
    val sparkConf = new SparkConf().setAppName("StatisticsRecommender").setMaster(config("spark.cores"))

    //SparkSession
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()

    val mongoConfig = MongoConfig(config("mongo.uri"), config("mongo.db"))

    //加入隐式转换
    import spark.implicits._

    //数据加载进来
    val ratingDF = spark
      .read
      .option("uri", mongoConfig.uri)
      .option("collection", MONGODB_RATING_COLLECTION)
      .format("com.mongodb.spark.sql")
      .load()
      .as[Rating]
      .toDF()

    val movieDF = spark
      .read
      .option("uri", mongoConfig.uri)
      .option("collection", MONGODB_MOVIE_COLLECTION)
      .format("com.mongodb.spark.sql")
      .load()
      .as[Movie]
      .toDF()

    //创建一张名叫rating的表
    ratingDF.createOrReplaceTempView("rating")
    //统计所有历史数据中每个电影评分数

    //数据结构 ：mid，count
    val rateMoreMoviesDF = spark.sql("select mid,count(mid) as count from ratings group by mid")

    rateMoreMoviesDF
      .write
      .option("uri", mongoConfig.uri)
      .option("collection", RATE_MORE_MOVIES)
      .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save()

    //统计以月为单位每个电影的评分数
    //数据结构：mid，count，time

    //创建一个日期格式化工具
    val simpleDataFormat = new SimpleDateFormat("yyyyMM")

    //注册一个UDF函数，用于将timestamp转换成年月格式
    spark.udf.register("changeData", (x: Int) => simpleDataFormat.format(new Date(x * 1000L)).toInt)

    //将原来的Rating数据集中的时间转换成年月格式
    val ratingOfYeahMouth=spark.sql("select mid,score,changeDate(timestamp) as yeahmouth from ratings")

    //将新的数据集注册成为一张表
    ratingOfYeahMouth.createOrReplaceTempView("ratingOfMouth")

    val rateMoreRecentlyMovies=spark.sql("select mid,counts(mid) as counts,yeahmouth from ratingOdMouth group by yearmouth ,mid")

    rateMoreRecentlyMovies
      .write
      .option("uri",mongoConfig.uri)
      .option("collection",RATA_MORE_RECENTLY_MOVIES)
      .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save()


    //统计每个电影的平均评分

    //统计每种电影类型中评分最高的十个电影

    //关闭spark

  }

}
