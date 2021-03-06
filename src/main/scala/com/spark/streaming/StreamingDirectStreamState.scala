package com.spark.streaming

import com.alibaba.fastjson.JSON
import com.spark.config.Config
import kafka.serializer.StringDecoder
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, State, StateSpec, StreamingContext}

/**
  * Created by AnLei on 2017/7/17.
  */
object StreamingDirectStreamState {

  def main(args: Array[String]): Unit = {
    val topic = Config.KAFKA_TOPIC
    val brokers = Config.KAFKA_BROKERS
    val group = Config.KAFKA_GROUP

    val conf = new SparkConf().setAppName("StreamingDirectStream").setMaster("local[*]")
    val ssc = new StreamingContext(conf, Seconds(5))
    ssc.checkpoint("checkpoint")

    val kafkaParams = Map[String, String](
      "metadata.broker.list" -> brokers,
      "serializer.class" -> "kafka.serializer.StringEncoder",
      "auto.offset.reset" -> "largest",
      "group.id" -> group)

    val topicsSet = topic.split(",").toSet
    val kafkaStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topicsSet)

    val events = kafkaStream.flatMap(line => {
      println(s"line ${line}.")
      val data = JSON.parseObject(line._2)
      Some(data)
    })

    val dStream = events.map(x => (x.getString("name"), x.getLong("count").toInt))

    /**
      * updateStateByKey
      */
//    val updateFunc = (currValues: Seq[Int], state: Option[Int]) => {
//      val currentCount = currValues.foldLeft(0)(_ + _)
//      // val currentCount = currValues.sum
//
//      val previousCount = state.getOrElse(0)
//      Some(currentCount + previousCount)
//    }
//    val counts = dStream.updateStateByKey(updateFunc)

    /**
      * mapWithState
      */
    val initialRDD = ssc.sparkContext.parallelize(List[(String, Int)]())
    val mappingFunc = (word: String, one: Option[Int], state: State[Int]) => {
      val sum = one.getOrElse(0) + state.getOption.getOrElse(0)
      val output = (word, sum)
      state.update(sum)
      output
    }
    val counts = dStream.mapWithState(StateSpec.function(mappingFunc).initialState(initialRDD)).reduceByKey((m,n) => {
      if(m > n) {
        m
      } else {
        n
      }
    })

    counts.foreachRDD(rdd => {
      rdd.foreachPartition(records => {
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
    })

    ssc.start()
    ssc.awaitTermination()

  }

}
