package com.spark.config

/**
  * Created by AnLei on 2017/5/15.
  */
object Config {
  val KAFKA_BROKERS: String = "ha01:9092,ha02:9092,ha03:9092"
  val KAFKA_TOPIC: String = "user"

  val REDIS_SERVER: String = "ha01"
  val REDIS_PORT: Int = 6379
}
