package com.spark.sql

import org.apache.spark.sql.SparkSession

/**
  * Created by AnLei on 2017/4/29.
  */
object BasketballDataFrame {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().master("local").appName("BasketballDataFrame").getOrCreate()

  }
}
