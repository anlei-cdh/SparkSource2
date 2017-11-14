package com.spark.ml.classification

import org.apache.spark.sql.SparkSession

/**
  * Created by AnLei on 2017/11/14.
  *
  * LogisticRegression
  */
object LogisticRegression {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().master("local").appName(s"${this.getClass.getSimpleName}").getOrCreate()

  }

}
