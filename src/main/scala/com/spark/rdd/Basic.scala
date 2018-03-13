package com.spark.rdd

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by AnLei on 2017/4/12.
  * Basic
  */
object Basic {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("basic").setMaster("local")
    val sc = new SparkContext(conf)

    val listRdd = sc.parallelize(List(1,2,3),2)
    /**
      * map
      */
    println("map")
    listRdd.map(x => x * x).collect().foreach(print(_) + "" + print(" "))
    println()
    /**
      * mapPartitions
      */
    println("mapPartitions")
    listRdd.mapPartitions(x => {
      var sum = 0
      for(i <- x) {
        sum += i
      }
      Iterator(sum)
    }).collect().foreach(print(_) + "" + print(" "))
    println()
    /**
      * filter
      */
    println("filter")
    listRdd.filter(_ % 2 == 0).foreach(print(_) + "" + print(" "))
    println()
    /**
      * flatMap
      */
    println("flatMap")
    listRdd.flatMap(x => 1 to x).foreach(print(_) + "" + print(" "))
    println()
    /**
      * glom函数将每个分区形成一个数组
      */
    println("glom")
    listRdd.glom.foreach(x => {
      for(i <- x) {
        print(i)
      }
      print(" ")
    })
    println()
    /**
      * subtract
      */
    println("subtract")
    val listRdd2 = sc.parallelize(List(3,4,5),2)
    listRdd.subtract(listRdd2).collect().foreach(print(_) + "" + print(" "))
    println()
    /**
      * sample
      * 是否有有放回的抽样
      * 百分比
      * 随机种子
      */
    println("sample")
    listRdd.sample(false,0.5,1).collect().foreach(print(_) + "" + print(" "))
    println()
    /**
      * takeSample
      * 是否有有放回的抽样
      * 百分比
      * 随机种子
      */
    println("takeSample")
    listRdd.takeSample(false,2,5).foreach(print(_) + "" + print(" "))
    println()

    sc.stop()
  }

}
