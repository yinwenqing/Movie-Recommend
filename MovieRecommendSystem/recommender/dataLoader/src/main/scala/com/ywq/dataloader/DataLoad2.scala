package com.ywq.dataloader

import java.net.InetAddress

import com.mongodb.casbah.commons.MongoDBObject
import com.mongodb.casbah.{MongoClient, MongoClientURI}
import com.ywq.scala.model._
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.common.transport.InetSocketTransportAddress
import org.elasticsearch.transport.client.PreBuiltTransportClient
import com.ywq.java.model.Constant._


/**
  * 导入测试数据到MongoDB
 */
object DataLoad2 {

  val MOVIE_DATA_PATH = "C:\\Users\\ywq\\Desktop\\Movie-Recommend\\MovieRecommendSystem\\recommender\\dataLoader\\src\\main\\resources\\small\\movies2.csv"
  val RATING_DATA_PATH = "C:\\Users\\ywq\\Desktop\\Movie-Recommend\\MovieRecommendSystem\\recommender\\dataLoader\\src\\main\\resources\\small\\rating2.csv"
  val TAGS_DATA_PATH = "C:\\Users\\ywq\\Desktop\\Movie-Recommend\\MovieRecommendSystem\\recommender\\dataLoader\\src\\main\\resources\\small\\tags.csv"


  //程序的入口
  def main(args: Array[String]): Unit = {

    val config = Map(
      "spark.cores" -> "local[*]",
      "mongo.uri" -> "mongodb://ywq5:27017/recommender",
      "mongo.db" -> "recommender"
    )

    //需要创建一个SparkConf配置
    val sparkConf = new SparkConf().setAppName("DataLoader").setMaster(config.get("spark.cores").get);

    //创建一个SparkSession
    val spark = SparkSession.builder().config(sparkConf).getOrCreate();

    import spark.implicits._

    //将movie、rating、tag数据集加载近来
    val movieRDD = spark.sparkContext.textFile(MOVIE_DATA_PATH);
    //将MovieRDD装换为DataFrame
    val movieDF = movieRDD.map(item => {
      val attr = item.split("\\^")
      Movie(attr(0).toInt, attr(1).trim, attr(2).trim, attr(3).trim, attr(4).trim, attr(5).trim, attr(6).trim, attr(7).trim, attr(8).trim, attr(9).trim);
    }).toDF()

    val ratingRDD = spark.sparkContext.textFile(RATING_DATA_PATH);
    //将RatingRDD装换为DataFrame
    val ratingDF = ratingRDD.map(item => {
      val attr = item.split(",")
      MovieRating(attr(0).toInt, attr(1).toInt, attr(2).toDouble, attr(3).toInt)
    }).toDF()

    val tagRDD = spark.sparkContext.textFile(TAGS_DATA_PATH);
    //将TagRDD装换为DataFrame
    val tagDF = tagRDD.map(item => {
      val attr = item.split(",")
      Tag(attr(0).toInt, attr(1).toInt, attr(2).toString, attr(3).toInt)
    }).toDF()

    implicit val mongoConfig = MongoConfig(config.get("mongo.uri").get, config.get("mongo.db").get)

    //需要把数据保存到MongoDB中
    //    storeDataInMongoDB(movieDF, ratingDF, tagDF)

    /**
      * Movie数据集，数据集字段通过^分割
      * 9^                            电影的ID
      * Sudden Death (1995)^          电影的名称
      * ……^                         电影的描述
      * 106 minutes^                  电影的时长
      * February 10, 1997^            电影的发行日期
      * 1995^                         电影的拍摄日期
      * English ^                     电影的语言
      * Action ^                      电影的类型
      * Jean-Claude ...^              电影的主要演员
      * Peter Hyams                   电影的导演
      *
      * tag1|tag2|tag3                电影的Tag
      */
    //首先需要将Tag数据集进行处理，处理后的形式为Mid，tag1|tag2|tag...
    import org.apache.spark.sql.functions._
    /**
      * new Tag:
      * Mid,tags
      * 1   tag1|tag2|...
      */
    val newTag = tagDF.groupBy($"mid").agg(concat_ws("|", collect_set($"tag")).as("tags")).select("mid", "tags")

    //需要将处理后的Tag数据，和Movie数据融合，产生新的Movie数据，
    //如果没有“left”，会只保存两个数据集都有的部分，设置joinType为left就是左边的数据集所有列都保存
    val movieWithTagDF = movieDF.join(newTag, Seq("mid", "mid"), "left")

    //需要把数据保存到MongoDB中
    storeDataInMongoDB(movieDF, ratingDF)

    //关闭spark
    spark.stop()

  }

  //将数据保存到MongoDB中的方法
  def storeDataInMongoDB(movieDF: DataFrame, ratingDF: DataFrame)(implicit mongoConfig: MongoConfig): Unit = {
    //新建一个到MongoDB的连接
    val mongoClient = MongoClient(MongoClientURI(mongoConfig.uri))

    //如果MongoDB中有对应的数据库，那么应该删除
    mongoClient(mongoConfig
      .db)(MONGO_MOVIE_COLLECTION).dropCollection()
    mongoClient(mongoConfig.db)(MONGO_RATING_COLLECTION).dropCollection()

    //将当前数据写入到MongoDB
    movieDF
      .write
      .option("uri", mongoConfig.uri)
      .option("collection", MONGO_MOVIE_COLLECTION)
      .mode("overwrite")
      .format(MONGO_DRIVER_CLASS)
      .save()

    ratingDF
      .write
      .option("uri", mongoConfig.uri)
      .option("collection", MONGO_RATING_COLLECTION)
      .mode("overwrite")
      .format(MONGO_DRIVER_CLASS)
      .save()

    //对数据表建索引
    mongoClient(mongoConfig.db)(MONGO_MOVIE_COLLECTION).createIndex(MongoDBObject("mid" -> 1))
    mongoClient(mongoConfig.db)(MONGO_RATING_COLLECTION).createIndex(MongoDBObject("uid" -> 1))
    mongoClient(mongoConfig.db)(MONGO_RATING_COLLECTION).createIndex(MongoDBObject("mid" -> 1))

    //关闭MongoDB的连接
    mongoClient.close()
  }

}
