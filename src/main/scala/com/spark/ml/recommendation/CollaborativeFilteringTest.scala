package com.spark.ml.recommendation

import org.apache.spark.sql.SparkSession

object CollaborativeFilteringTest {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local").appName(s"${this.getClass.getSimpleName}").getOrCreate()

    val df = spark.createDataFrame(
      Seq(
        (28, List(List(25,5.689864),List(92,5.360779),List(76,5.1021585))),
        (26, List(List(51,6.298293),List(22,5.4222317),List(94,5.2276535))),
        (27, List(List(18,3.7351623),List(7,3.692539),List(23,3.3052857))),
        (12, List(List(46,9.0876255),List(17,4.984369),List(35,4.9596915))),
        (22, List(List(53,5.329093),List(74,5.013483),List(75,4.916749)))
      )
    ).toDF("userId", "recommendations")

    val result = df.selectExpr("userId", "explode(recommendations) AS reco")

    result.repartition(1).foreachPartition(records => {
      if (!records.isEmpty) {
        records.foreach {
          record => {
            val userId = record.getAs[Int]("userId")
            val reco = record.getAs[Seq[Double]]("reco")
            println(userId + " - " + reco(0).toInt + " - " + reco(1))
          }
        }
      }
    })
    spark.stop()
  }

}
