package com.spark.streaming

import java.util.Properties

import com.alibaba.fastjson.JSONObject
import com.spark.config.Config
import kafka.javaapi.producer.Producer
import kafka.producer.{KeyedMessage, ProducerConfig}

/**
  * Created by AnLei on 2017/7/17.
  */
object KafkaProducer {

  private val users = Array(
    "张三", "李四",
    "王五", "赵六",
    "068b746ed4620d25e26055a9f804385f", "97edfc08311c70143401745a03a50706",
    "d7f141563005d1b5d0d3dd30138f3f62", "c8ee90aade1671a21336c721512b817a",
    "6b67c8c700427dee7552f81f3228c927", "a95f22eabc4fd4b580c011a3161a9d9d")

  def main(args: Array[String]): Unit = {
    val topic = Config.KAFKA_TOPIC
    val brokers = Config.KAFKA_BROKERS

    val properties: Properties = new Properties
    properties.put("metadata.broker.list", brokers)
    properties.put("serializer.class", "kafka.serializer.StringEncoder")

    val kafkaConfig = new ProducerConfig(properties)
    val producer = new Producer[String, String](kafkaConfig)

    var index = 1
    while(true){
      val event = new JSONObject()
      event.put("id", index)
      event.put("time", System.currentTimeMillis.toString)
      event.put("count", 1)
      index += 1

      val message: KeyedMessage[String, String] = new KeyedMessage[String, String](topic, event.toString)
      producer.send(message)
      println(event)

      Thread.sleep(200)
    }
  }

}
