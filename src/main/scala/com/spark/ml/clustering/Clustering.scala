package com.spark.ml.clustering

import org.apache.spark.sql.SparkSession

/**
  * Created by AnLei on 2017/11/14.
  *
  * Clustering
  */
object Clustering {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().master("local").appName(s"${this.getClass.getSimpleName}").getOrCreate()

  }
}
