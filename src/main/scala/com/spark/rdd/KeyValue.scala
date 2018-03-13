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

    /**
      * reduceByKey
      */
    pets.reduceByKey(_+_).foreach(println(_))
    println()
    /**
      * groupByKey
      */
    pets.groupByKey().foreach(println(_))
    println()
    /**
      * sortByKey
      */
    pets.sortByKey().foreach(println(_))
    println()
    /**
      * mapValues
      * 针对(Key,Value)型数据中的Value进行Map操作，而不对Key进行处理。
      */
    pets.mapValues(x => x + "_").foreach(println(_))
    println()
    /**
      * combineByKey
      */
    pets.combineByKey(
      x => x,
      (x: Int, y: Int) => x + y,
      (x: Int, y: Int) => x + y
    ).foreach(println(_))
    println()
    /**
      * Cogroup
      */
    val pets2 = sc.parallelize(List(("cat", 3),("dog", 4),("pig", 5)))
    pets.cogroup(pets2).foreach(println(_))

    sc.stop()
  }

}
