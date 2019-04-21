package com.ywq.dataloader

import java.net.InetAddress

import com.mongodb.casbah.commons.MongoDBObject
import com.mongodb.casbah.{MongoClient, MongoClientURI}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequest
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.common.transport.InetSocketTransportAddress
import org.elasticsearch.transport.client.PreBuiltTransportClient

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

case class Tag(val uid: Int, val mid: Int, val tag: String, val timestamp: Int)

/**
  * MongoDB 的连接配置
  *
  * @param url MongoDB的连接
  * @param db  MongoDB要操作的数据库
  */
case class MongoConf(val uri: String, val db: String)

/**
  * ES的连接配置
  *
  * @param httpHosts      Http的主机列表，以“，”分割
  * @param transportHosts Transport主机列表，以“，”分割
  * @param index          需要操作的索引
  * @param clusterName    ES集群的名称
  */
case class ESConfig(val httpHosts: String, val transportHosts: String, val index: String, val clusterName: String)

//数据的主加载服务
object DataLoader {

  val MOVIE_DATA_PATH = "C:\\Users\\ywq\\Desktop\\Movie-Recommend\\MovieRecommendSystem\\recommender\\dataLoader\\src\\main\\resources\\small\\movies.csv"
  val RATING_DATA_PATH = "C:\\Users\\ywq\\Desktop\\Movie-Recommend\\MovieRecommendSystem\\recommender\\dataLoader\\src\\main\\resources\\small\\ratings.csv"
  val TAGS_DATA_PATH = "C:\\Users\\ywq\\Desktop\\Movie-Recommend\\MovieRecommendSystem\\recommender\\dataLoader\\src\\main\\resources\\small\\tags.csv"

  val MONGODB_MOVIE_COLLECTION = "Movie"
  val MONGODB_RATING_COLLECTION = "Rating"
  val MONGODB_TAG_COLLECTION = "Tag"

  val ES_MOVIE_INDEX = ""


  //程序的入口
  def main(args: Array[String]): Unit = {

    val config = Map(
      "spark.cores" -> "local[*]",
      "mongo.uri" -> "mongodb://192.168.43.31:27017/recommender",
      "mongo.db" -> "recommender",
      "es.httpHosts" -> "192.168.43.31:9200",
      "es.transportHosts" -> "192.168.43.31:9300",
      "ex.index" -> "recommender",
      "es.clusterName" -> "es-cluster"
    )

    //需要创建爱你一个SparkConf配置
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
    //将MovieRDD装换为DataFrame
    val ratingDF = ratingRDD.map(item => {
      val attr = item.split(",")
      Rating(attr(0).toInt, attr(1).toInt, attr(2).toDouble, attr(3).toInt)
    }).toDF()

    val tagRDD = spark.sparkContext.textFile(TAGS_DATA_PATH);
    //将MovieRDD装换为DataFrame
    val tagDF = tagRDD.map(item => {
      val attr = item.split(",")
      Tag(attr(0).toInt, attr(1).toInt, attr(2).toString, attr(3).toInt)
    }).toDF()

    implicit val mongoConfig = MongoConf(config.get("mongo.uri").get, config.get("mongo.db").get)

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

    //声明了一个ES配置的参数
    implicit val esConfig = ESConfig(config.get("es.httpHosts").get, config.get("es.transportHosts").get,
      config.get("ex.index").get, config.get("es.clusterName").get)

    //需要将新的Movie数据保存到ES中
    storeDataInES(movieWithTagDF)

    //关闭spark
    spark.stop()

  }

  //将数据保存到MongoDB中的方法
  def storeDataInMongoDB(movieDF: DataFrame, ratingDF: DataFrame, tagDF: DataFrame)(implicit mongoConfig: MongoConf): Unit = {
    //新建一个到MongoDB的连接
    val mongoClient = MongoClient(MongoClientURI(mongoConfig.uri))

    //如果MongoDB中有对应的数据库，那么应该删除
    mongoClient(mongoConfig.db)(MONGODB_MOVIE_COLLECTION).dropCollection()
    mongoClient(mongoConfig.db)(MONGODB_RATING_COLLECTION).dropCollection()
    mongoClient(mongoConfig.db)(MONGODB_TAG_COLLECTION).dropCollection()

    //将当前数据写入到MongoDB
    movieDF
      .write
      .option("uri", mongoConfig.uri)
      .option("collection", MONGODB_MOVIE_COLLECTION)
      .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save()

    ratingDF
      .write
      .option("uri", mongoConfig.uri)
      .option("collection", MONGODB_RATING_COLLECTION)
      .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save()

    tagDF
      .write
      .option("uri", mongoConfig.uri)
      .option("collection", MONGODB_TAG_COLLECTION)
      .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save()

    //对数据表建索引
    mongoClient(mongoConfig.db)(MONGODB_MOVIE_COLLECTION).createIndex(MongoDBObject("mid" -> 1))
    mongoClient(mongoConfig.db)(MONGODB_RATING_COLLECTION).createIndex(MongoDBObject("uid" -> 1))
    mongoClient(mongoConfig.db)(MONGODB_RATING_COLLECTION).createIndex(MongoDBObject("mid" -> 1))
    mongoClient(mongoConfig.db)(MONGODB_TAG_COLLECTION).createIndex(MongoDBObject("uid" -> 1))
    mongoClient(mongoConfig.db)(MONGODB_TAG_COLLECTION).createIndex(MongoDBObject("mid" -> 1))

    //关闭MongoDB的连接
    mongoClient.close()
  }

  //将数据保存到ES中的方法
  def storeDataInES(movieDF: DataFrame)(implicit esConfig: ESConfig) = {
    //新建一个配置
    val settings: Settings = Settings.builder().put("cluster.name", esConfig.clusterName).build()

    //新建一个ES客户端
    val esClient = new PreBuiltTransportClient(settings)

    //需要将TransportHosts添加到ESClient中
    val REGEX_HOSTS_PORT = "(.+):(\\d+)".r
    esConfig.transportHosts.split(",").foreach {
      case REGEX_HOSTS_PORT(host: String, port: String) => {
        esClient.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(host), port.toInt))
      }
    }

    //需要清除掉ES中的遗留数据
    //    if (esClient.admin().indices().exists(new IndicesExistsRequest(esConfig.index)).actionGet().isExists) {
    //      esClient.admin().indices().delete(new DeleteIndexRequest(esConfig.index))
    //    }
    //    esClient.admin().indices().create(new CreateIndexRequest(esConfig.index))

    //将数据写入到ES中
    movieDF
      .write
      .option("es.nodes", esConfig.httpHosts)
      .option("es.http.timeout", "100m")
      .option("es.mapping.id", "mid")
      .mode("overwrite")
      .format("org.elasticsearch.spark.sql")
      .save(esConfig.index + "/" + ES_MOVIE_INDEX)
  }

}
