package com.spark.rdd

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by AnLei on 2017/4/12.
  * KeyValue的Rdd
  */
object KeyValue {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("keyvalue").setMaster("local")
    val sc = new SparkContext(conf)
    val pets = sc.parallelize(List(("cat", 1),("dog", 1),("cat", 2)))

    // reduceByKey
    pets.reduceByKey(_+_).foreach(println(_))
    println()
    // groupByKey
    pets.groupByKey().foreach(println(_))
    println()
    // groupByKey map
    pets.groupByKey().map(m => {
      val word = m._1
      var count = 0
      m._2.foreach(item => {
        count += item
      })
      (word, count)
    }).foreach(println(_))
    println()
    // sortByKey
    pets.sortByKey().foreach(println(_))
    println()
    // countByKey
    pets.countByKey().foreach(println(_))
    println()
    // countByValue
    pets.countByValue().foreach(println(_))
    println()
    
    sc.stop()
  }

}
