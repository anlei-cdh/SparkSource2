package com.spark.ml.recommendation

import org.apache.spark.sql.SparkSession

object CollaborativeFilteringTest {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local").appName(s"${this.getClass.getSimpleName}").getOrCreate()

    val df = spark.createDataFrame(
      Seq(
        (125, List(8.0,8.0,1.0,8.0), 1),
        (124, List(1.0,2.0,6.0,2.0,3.0), 1),
        (123, List(1.0,1.0,6.0,3.0,3.0), 2),
        (122, List(1.0,1.0,6.0,5.0,1.0), 2),
        (121, List(1.0,4.0,6.0,4.0,1.0), 3)
      )
    ).toDF("label", "textlist", "prediction")

    spark.stop()
  }

}
