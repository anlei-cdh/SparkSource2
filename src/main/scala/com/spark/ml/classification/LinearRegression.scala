package com.spark.ml.classification

import org.apache.spark.sql.SparkSession

object LinearRegression {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().master("local").appName(s"${this.getClass.getSimpleName}").getOrCreate()

  }

}
