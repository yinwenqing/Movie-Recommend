package com.ywq.streaming

import com.mongodb.casbah.commons.MongoDBObject
import com.mongodb.casbah.{MongoClient, MongoClientURI}
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis

import scala.collection.JavaConversions._
import com.ywq.java.model.Constant._

object ConnHelper extends Serializable {
  lazy val jedis = new Jedis("ywq5")
  lazy val mongoClient = MongoClient(MongoClientURI("mongodb://ywq5:27017/recommender"))
}

case class MongoConfig(uri: String, db: String)

//推荐
case class Recommendation(rid: Int, r: Double)

// 用户的推荐
case class UserRecs(uid: Int, recs: Seq[Recommendation])

//电影的相似度
case class MovieRecs(uid: Int, recs: Seq[Recommendation])

object StreamingRecommender {

  val MAX_USER_RATING_NUM = 20
  val MAX_SIM_MOVIES_NUM = 20

  //入口方法
  def main(args: Array[String]): Unit = {

    val config = Map(
      "spark.cores" -> "local[*]",
      "mongodb.uri" -> "mongodb://ywq5:27017/recommender",
      "mongodb.db" -> "recommender",
      "kafka.topic" -> "recommender"
    )

    //创建一个SparkConf配置
    val sparkConf = new SparkConf().setAppName("StreamingRecommender").setMaster(config("spark.cores"))

    //创建Spark对象
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    val sc = spark.sparkContext
    val ssc = new StreamingContext(sc, Seconds(2))

    implicit val mongoConfig = MongoConfig(config("mongodb.uri"), config("mongodb.db"))
    import spark.implicits._

    //广播电影相似度矩阵
    val simMoviesMatrix = spark
      .read
      .option("uri", config("mongodb.uri"))
      .option("collection", MONGO_MOVIE_RECS_COLLECTION)
      .format(MONGO_DRIVER_CLASS)
      .load()
      .as[MovieRecs]
      .rdd
      .map { recs =>
        (recs.uid, recs.recs.map(x => (x.rid, x.r)).toMap)
      }.collectAsMap()

    var simMoviesMatrixBroadCast = sc.broadcast(simMoviesMatrix)


    //创建到Kafka的连接
    val kafkaPara = Map(
      "bootstrap.servers" -> "ywq5:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "recommender",
      "auto.offset.reset" -> "latest"
    )

    val kafkaStream = KafkaUtils.createDirectStream[String, String](ssc, LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](Array(config("kafka.topic")), kafkaPara))

    //要向kafka传入的是：UID|MID|SCORE|TIMESTAMP
    //产生评分流
    val ratingStream = kafkaStream.map { case msg =>
      var attr = msg.value().split("\\|")
      (attr(0).toInt, attr(1).toInt, attr(2).toDouble, attr(3).toInt)
    }

    ratingStream.foreachRDD { rdd =>
      rdd.map { case (uid, mid, score, timestamp) =>
        println(">>>>>>>>>>>>>>")

        //获取当前用户最近M评分
        val userRecentlyRatings = getUserRecentlyRating(MAX_USER_RATING_NUM, uid, ConnHelper.jedis)

        //获取电影p最相似的k个电影
        val simMovies = getTopSimMovies(MAX_SIM_MOVIES_NUM, mid, uid, simMoviesMatrixBroadCast.value)

        //计算待选电影的推荐优先级
        val streamRecs=computerMovieScores(simMoviesMatrixBroadCast.value,userRecentlyRatings,simMovies)

        //将数据保存到MongoDB中
        saveRecsToMongoDB(uid,streamRecs)

      }.count()
    }


    //启动Streaming程序
    ssc.start()
    ssc.awaitTermination()
  }

  /**
    * 获取当前用户最近的M次评分
    *
    * @param num 评分的个数
    * @param uid 用户
    * @return
    */
  def getUserRecentlyRating(num: Int, uid: Int, jedis: Jedis): Array[(Int, Double)] = {

    //从用户的队列中取出num个评论
    jedis.lrange("uid:" + uid.toString, 0, num).map { item =>
      val attr = item.split("\\|")
      (attr(0).trim.toInt, attr(1).trim.toDouble)
    }.toArray
  }

  /**
    * 获取当前电影K个相似的电影
    *
    * @param num         相似电影的数量
    * @param mid         当前电影的ID
    * @param uid         当前的评分用户
    * @param simMovies   电影相似度矩阵的广播变量值
    * @param mongoConfig MongoDB的配置
    * @return
    */
  def getTopSimMovies(num: Int, mid: Int, uid: Int, simMovies: scala.collection.Map[Int, scala.collection.immutable.Map[Int, Double]])(implicit mongoConfig: MongoConfig) = {
    //从广播变量的电影相似度矩阵中获得当前电影所有的相似电影
    val allSimMovies = simMovies.get(mid).get.toArray

    //获取用户已经观看过的电影
    val ratingExist = ConnHelper.mongoClient(mongoConfig.db)(MONGO_RATING_COLLECTION).find(MongoDBObject("uid")).toArray.map { item =>
      item.get("mid").toString.toInt
    }

    //过滤掉已经评分过的电影，并排序输出
    allSimMovies.filter(x => !ratingExist.contains(x._1)).sortWith(_._2 > _._2).take(num).map(x => x._2)
    null
  }

  /**
    * 获取两个电影之间的相似度
    * @param simMovies            电影相似度矩阵
    * @param userRatingMovie      用户已经评分的电影
    * @param topSimMovie          候选电影
    * @return
    */
  def getMoviesSimScore(simMovies: scala.collection.Map[Int, scala.collection.immutable.Map[Int, Double]], userRatingMovie: Int, topSimMovie: Int): Double = {

    simMovies.get(topSimMovie) match{
      case Some(sim)=>sim.get(userRatingMovie) match {
        case Some(score)=>score
        case None=>0
      }
      case None =>0.0
    }

  }

  //取2的对数
  def log(m:Int):Double={
    math.log(m)/math.log(2)
  }

  /**
    * 计算待选电影的推荐分数
    * @param simMovies                电影相似度矩阵
    * @param userRecentlyRatings     用户最近的k次评分
    * @param topSimMovies             当前电影最相似的k个电影
    * @return
    */
  def computerMovieScores(simMovies: scala.collection.Map[Int, scala.collection.immutable.Map[Int, Double]], userRecentlyRatings: Array[(Int, Double)], topSimMovies: Array[Int]): Array[(Int, Double)] = {
    //用于保存每一个待选电影和最近评分的每一个电影的权重得分
    val score = scala.collection.mutable.ArrayBuffer[(Int, Double)]()

    //用于保存每一个电影的增强因子数
    val increMap = scala.collection.mutable.HashMap[Int, Int]()

    //用于保存每个电影的减弱因子数
    val decreMap = scala.collection.mutable.HashMap[Int, Int]()

    for (topSimMovie <- topSimMovies; userRecentlyRating <- userRecentlyRatings) {
      val simScore = getMoviesSimScore(simMovies, userRecentlyRating._1, topSimMovie)
      if (simScore > 0.6) {
        score += ((topSimMovie, simScore * userRecentlyRating._2))
        if (userRecentlyRating._2 > 3) {
          increMap(topSimMovie) = increMap.getOrDefault(topSimMovie, 0) + 1
        } else {
          decreMap(topSimMovie) = decreMap.getOrDefault(topSimMovie, 0) + 1
        }
      }
    }

    score.groupBy(_._1).map{case (mid,sims)=>
      (mid,sims.map(_._2).sum/sims.length + log(increMap(mid)) - log(decreMap(mid))
      )}.toArray
  }

  /**
    * 将数据保存到MongoDB       uid -> 1,  recs -> 22:4.3|23:2.3
    * @param streamRecs     流式的推荐结果
    * @param mongoConfig    MongoDB的配置
    * @return
    */
  def saveRecsToMongoDB(uid:Int,streamRecs:Array[(Int,Double)])(implicit mongoConfig: MongoConfig):Unit={
    //到StreamRecs的连接
    val streamRecsCollection=ConnHelper.mongoClient(mongoConfig.db)(MONGO_STREAM_RECS_COLLECTION)

    streamRecsCollection.findAndRemove(MongoDBObject("uid"->uid))
    streamRecsCollection.insert(MongoDBObject("uid"->uid,"recs"->streamRecs.map(x=>x._1+":"+x._2).mkString("|")))
  }

}
