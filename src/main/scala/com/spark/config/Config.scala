package com.spark.config

/**
  * Created by AnLei on 2017/5/15.
  */
object Config {
  val ZOOKEEPER_QUORUM: String = "ha01:2181,ha02:2181,ha03:2181"

  val KAFKA_BROKERS: String = "ha01:9092,ha02:9092,ha03:9092"
  val KAFKA_TOPIC: String = "topic2"
  val KAFKA_GROUP: String = "group2"
  val KAFKA_USER_TOPIC: String = "user"
  val KAFKA_RECO_TOPIC: String = "reco6"

  val REDIS_SERVER: String = "ha01"
  val REDIS_PORT: Int = 6379

  val testgit = true;
}
