package com.spark.ml.classification

import org.apache.spark.sql.SparkSession
import org.apache.spark.ml.classification.LogisticRegression

/**
  * Created by AnLei on 2017/8/4.
  */
object BLogisticRegression {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().master("local").appName(s"${this.getClass.getSimpleName}").getOrCreate()
    
  }

}
