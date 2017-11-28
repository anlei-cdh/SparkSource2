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

  def main(args: Array[String]): Unit = {

  }

}
