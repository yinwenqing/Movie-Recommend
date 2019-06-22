package com.ywq.scala.model

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
case class MovieRating(val uid: Int, val mid: Int, val score: Double, val timestamp: Int)

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
case class MongoConfig(val uri: String, val db: String)

/**
  * ES的连接配置
  *
  * @param httpHosts      Http的主机列表，以“，”分割
  * @param transportHosts Transport主机列表，以“，”分割
  * @param index          需要操作的索引
  * @param clusterName    ES集群的名称
  */
case class ESConfig(val httpHosts: String, val transportHosts: String, val index: String, val clusterName: String)

//推荐
case class Recommendation(rid: Int, r: Double)

// 用户的推荐
case class UserRecs(uid: Int, recs: Seq[Recommendation])

//电影的相似度
case class MovieRecs(uid: Int, recs: Seq[Recommendation])

/**
  * 电影类别的推荐
  *
  * @param genres 电影的类别
  * @param recs   top10的电影的集合
  */
case class GenresRecommendation(genres: String, recs: Seq[Recommendation])

object Model {


}
