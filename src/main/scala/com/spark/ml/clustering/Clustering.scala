package com.spark.ml.clustering

import org.apache.spark.sql.SparkSession

object Clustering {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().master("local").appName(s"${this.getClass.getSimpleName}").getOrCreate()

  }
}
