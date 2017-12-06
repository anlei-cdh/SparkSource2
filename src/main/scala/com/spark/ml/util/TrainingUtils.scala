package com.spark.ml.util

object TrainingUtils {

  val trainingData = Seq(
    (1, "Hi I heard about Spark", 0.0),
    (2, "I wish Java could use case classes", 1.0),
    (3, "Logistic regression models are neat", 2.0)
  )

  val testData = Seq(
    (1, "Hi I'd like spark"),
    (2, "I wish Java could use goland"),
    (3, "Linear regression models are neat"),
    (4, "Logistic regression models are neat")
  )

  val clusteringData = Seq(
    (1001, "时政 国际 军事 体育 体育 国际 体育"),
    (1002, "娱乐 体育"),
    (1003, "体育 体育 国际 体育"),
    (1004, "军事 体育 体育"),
    (1005, "时政 财经 财经 体育 军事")
  )

  val clusteringData2 = Seq(
    (1001, "1.0 2.0 3.0 4.0 4.0 2.0 4.0"),
    (1002, "6.0 4.0"),
    (1003, "4.0 4.0 2.0 4.0"),
    (1004, "3.0 4.0 4.0"),
    (1005, "1.0 5.0 5.0 4.0 3.0")
  )

  def main(args: Array[String]): Unit = {

  }

}
