package com.spark.ml.classification

import org.apache.spark.sql.SparkSession

/**
  * Created by AnLei on 2017/8/4.
  */
object BLogisticRegression {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().master("local").appName(s"${this.getClass.getSimpleName}").getOrCreate()

  }

}
