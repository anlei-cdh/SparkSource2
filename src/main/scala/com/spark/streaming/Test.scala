package com.spark.streaming

/**
  * Created by AnLei on 2017/7/17.
  */
object Test {

  def main(args: Array[String]): Unit = {
    var index: Int = 0
    while (true) {
      index += 1
      System.out.println(index % 8)
      Thread.sleep(200)
    }
  }

}
