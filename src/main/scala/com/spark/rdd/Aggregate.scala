package com.spark.rdd

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by anlei on 2018/2/4.
  */
object Aggregate {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("cartesian").setMaster("local")
    val sc = new SparkContext(conf)

    def seqOP(a: Int, b: Int): Int = {
      println("seqOp: " + a + " " + b)
      math.min(a, b)
    }

    def combOp(a: Int, b: Int): Int = {
      println("combOp: " + a + " " + b)
      a + b
    }

    /**
      * Aggregate(5在每个分区都参与计算)
      * 分区1(1,2,3,4) -> 1
      * 分区2(5,6,7,8) -> 5
      * 聚集1 -> 10 + 1
      * 聚集2 -> 聚集1 + 5
      * 计算结果 -> 16
      * 如果分区数是1，计算结果为11(10 + 1)
      */
    val rdd = sc.parallelize(List(1, 2, 3, 4, 5, 6, 7, 8), 2)
    val result = rdd.aggregate(10)(seqOP, combOp)
    // val result = rdd.aggregate(5)((a, b) => (math.min(a, b)), (a, b) => (a + b))
    println("result: " + result)

    println("===============================")

    /**
      * Aggregate(10在每个分区都参与计算)
      * 分区1(1, 1), (1, 2), (1, 3), (1, 4) -> (1, 1)
      * 分区2(2, 5), (2, 6), (2, 7), (2, 8) -> (2, 5)
      * 此时的key正好是分区数，所以不需要聚集
      * 计算结果((2,5),(1,1))
      * 如果分区数是1，因为是按照key进行aggregate，也不需要聚集，计算结果不变
      */
    val rdd2 = sc.parallelize(List((1, 1), (1, 2), (1, 3), (1, 4), (2, 5), (2, 6), (2, 7), (2, 8)), 2)
    val result2 = rdd2.aggregateByKey(10)(seqOP, combOp)
    // val result2 = rdd2.aggregateByKey(5)((a, b) => (math.min(a, b)), (a, b) => (a + b))
    result2.collect().foreach(println(_))

    sc.stop()
  }

}
