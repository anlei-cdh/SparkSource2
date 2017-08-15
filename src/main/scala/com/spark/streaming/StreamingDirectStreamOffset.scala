package com.spark.streaming

import com.alibaba.fastjson.JSON
import com.spark.config.Config
import com.spark.util.kafkaManagerbycurator
import kafka.serializer.StringDecoder
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Created by AnLei on 2017/7/17.
  */
object StreamingDirectStreamOffset {

  def main(args: Array[String]): Unit = {
    val topic = Config.KAFKA_TOPIC
    val brokers = Config.KAFKA_BROKERS
    val group = Config.KAFKA_GROUP
    val zkQuorum = Config.ZOOKEEPER_QUORUM

    val conf = new SparkConf().setAppName("StreamingDirectStream").setMaster("local[*]")
    val ssc = new StreamingContext(conf, Seconds(5))
    val kafkaParams = Map[String, String](
      "metadata.broker.list" -> brokers,
      "serializer.class" -> "kafka.serializer.StringEncoder",
      "zookeeper.connect" -> zkQuorum,
      "group.id" -> group)

    val topicsSet = topic.split(",").toSet
    val km = new kafkaManagerbycurator(kafkaParams)
    val kafkaStream = km.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topicsSet)

    kafkaStream.foreachRDD(rdd => {
      if (!rdd.isEmpty()) {
        rdd.flatMap(line => {
          println(s"line ${line}.")
          val data = JSON.parseObject(line._2)
          Some(data)
        }).map(x => (x.getString("name"), x.getLong("count").toInt))
          .reduceByKey(_+_)
          .foreachPartition(records => {
            records.foreach(record => {
              try {
                val id = record._1
                val count = record._2
                println(s"id ${id} to ${count}.")
              } catch {
                case e: Exception => println("error:" + e)
              }
            })
          })
      }
      km.updateZKOffsets(rdd)
    })

    ssc.start()
    ssc.awaitTermination()

  }

}
