package com.ywq.statistics

import java.sql.Date
import java.text.SimpleDateFormat

import com.ywq.scala.model._
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import com.ywq.java.model.Constant._

object statisticsRecommender {

  //入口方法
  def main(args: Array[String]): Unit = {
    val config = Map(
      "spark.cores" -> "local[*]",
      "mongo.uri" -> "mongodb://192.168.43.31:27017/recommender",
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
      .option("collection", MONGO_RATING_COLLECTION)
      .format(MONGO_DRIVER_CLASS)
      .load()
      .as[MovieRating]
      .toDF()

    val movieDF = spark
      .read
      .option("uri", mongoConfig.uri)
      .option("collection", MONGO_MOVIE_COLLECTION)
      .format(MONGO_DRIVER_CLASS)
      .load()
      .as[Movie]
      .toDF()

    //创建一张名叫rating的表
    ratingDF.createOrReplaceTempView("rating")

    //统计所有历史数据中每个电影评分数
    //数据结构 ：mid，count
    val rateMoreMoviesDF = spark.sql("select mid,count(mid) as count from Rating group by mid")

    rateMoreMoviesDF
      .write
      .option("uri", mongoConfig.uri)
      .option("collection", MONGO_RATE_MORE_MOVIES)
      .mode("overwrite")
      .format(MONGO_DRIVER_CLASS)
      .save()

    //统计以月为单位每个电影的评分数
    //数据结构：mid，count，time

    //创建一个日期格式化工具
    val simpleDataFormat = new SimpleDateFormat("yyyyMM")

    //注册一个UDF函数，用于将timestamp转换成年月格式
    spark.udf.register("changeDate", (x: Int) => simpleDataFormat.format(new Date(x * 1000L)).toInt)

    //将原来的Rating数据集中的时间转换成年月格式
    val ratingOfYeahMouth = spark.sql("select mid,score,changeDate(timestamp) as yeahmouth from Rating")

    //将新的数据集注册成为一张表
    ratingOfYeahMouth.createOrReplaceTempView("ratingOfMouth")

    val rateMoreRecentlyMovies = spark.sql("select mid,count(mid) as counts,yeahmouth from ratingOfMouth group by yeahmouth ,mid")

    rateMoreRecentlyMovies
      .write
      .option("uri", mongoConfig.uri)
      .option("collection", MONGO_RATE_MORE_RECENTLY_MOVIES)
      .mode("overwrite")
      .format(MONGO_DRIVER_CLASS)
      .save()


    //统计每个电影的平均评分
    val averageMovieDF = spark.sql("select mid,avg(score) as avg from Rating group by mid")

    averageMovieDF
      .write
      .option("uri", mongoConfig.uri)
      .option("collection", MONGO_AVERAGE_MOVIES)
      .mode("overwrite")
      .format(MONGO_DRIVER_CLASS)
      .save()


    //统计每种电影类型中评分最高的十个电影
    //不需要用left join，因为只需要有评分的电影数据集
    val movieWithScore = movieDF.join(averageMovieDF, Seq("mid", "mid"))

    //所有的电影类别
    val genres = List("Action", "Adventue", "Animation", "Comedy", "Ccrime", "Documentary", "Drama",
      "Family", "Romance", "Science", "Tv", "Thriller", "War", "Western")

    //将电影类别转换为RDD
    val genresRDD = spark.sparkContext.makeRDD(genres)

    //计算电影类别Top10
    val genrenTopMovies = genresRDD.cartesian(movieWithScore.rdd)//将电影类别和电影数据进行笛卡尔积操作
      .filter {
        //过滤掉电影的类别不匹配的电影
        case (genres, row) => row.getAs[String]("genres").toLowerCase.contains(genres.toLowerCase)
      }
      .map {
        //将整个数据集的数据量减小，生成RDD[String,Iter[mid,avg]]
        case (genres, row) => {
          (genres, (row.getAs[Int]("mid"), row.getAs[Double]("avg")))
        }
      }.groupByKey()//将genres数据集中的相同的聚集
      .map {
      //通过评分的大小进行数据的排序，然后将数据映射为对象
        case (genres, items) => GenresRecommendation(genres, items.toList.sortWith(_._2 > _._2).take(10).map(item => Recommendation(item._1, item._2)))
      }.toDF()

    //输出数据到MongoDB
    genrenTopMovies
        .write
        .option("uri",mongoConfig.uri)
        .option("collection",MONGO_GENRES_TOP_MOVIES)
        .mode("overwrite")
        .format(MONGO_DRIVER_CLASS)
        .save()


    //关闭spark
    spark.stop()
  }

}
