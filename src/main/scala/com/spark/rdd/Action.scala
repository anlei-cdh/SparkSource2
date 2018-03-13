package com.spark.rdd

import org.apache.spark.{SparkConf, SparkContext}

object Action {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("basic").setMaster("local")
    val sc = new SparkContext(conf)
    val rdd = sc.parallelize(List("cat", "dog", "pig"), 2)
    val keyValue = sc.parallelize(List(("cat", 1),("dog", 1),("cat", 2)))

    /**
      * collect
      */
    rdd.collect().foreach(print(_) + "" + print(" "))
    println()
    /**
      * reduceByKey
      */
    keyValue.reduceByKey(_ + _).collect().foreach(print(_) + "" + print(" "))
    println()
    /**
      * reduceByKeyLocally
      */
    keyValue.reduceByKeyLocally(_ + _).foreach(print(_) + "" + print(" "))
    println()
    /**
      * lookup
      */
    keyValue.lookup("cat").foreach(print(_) + "" + print(" "))
    println()
    /**
      * reduce
      */
    val rdd2 = sc.parallelize(List(1, 2, 3),2)
    println(rdd2.reduce(_+_))
    /**
      * fold 相当于每次reduce都加1
      */
    println(rdd2.fold(1)(_+_))
    /**
      * aggregate 先对每个分区的所有元素进行aggregate操作，再对分区的结果进行fold操作
      */
    val aggregate = rdd2.aggregate((1))(
      (acc,number) => (acc + number),
      (par1,par2) => (par1 + par2)
    )
    println(aggregate)
    /**
      * aggregate2 先对每个分区的所有元素进行aggregate操作，再对分区的结果进行fold操作
      */
    val aggregate2 = rdd2.aggregate((1,1))(
      (acc,number) => (acc._1 + number, acc._2 + 1),
      (par1,par2) => (par1._1 + par2._1, par1._2 + par2._2)
    )
    println(aggregate2)

    sc.stop()
  }

}
