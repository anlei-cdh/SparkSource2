package com.spark.ml.classification

import com.spark.ml.util.MLUtils
import org.apache.spark.sql.SparkSession
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.classification.LogisticRegressionModel

/**
  * Created by AnLei on 2017/11/14.
  *
  * LogisticRegression
  */
object LogisticRegression {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().master("local").appName(s"${this.getClass.getSimpleName}").getOrCreate()

    val path = "model/lr"
    val numFeatures = 10000

    val trainingDataFrame = spark.createDataFrame(Seq(
      (1, "Hi I heard about Spark", 0.0),
      (2, "I wish Java could use case classes", 0.0),
      (3, "Logistic regression models are neat", 1.0)
    )).toDF("id", "text", "label")
    val training = MLUtils.idfFeatures(trainingDataFrame, numFeatures).select("label", "features")

    val lr = new LogisticRegression()
      .setMaxIter(10)
      .setRegParam(0.001)

    lr.fit(training).write.overwrite().save(path)

    val testDataFrame = spark.createDataFrame(Seq(
      (1, "Hi I'd like spark"),
      (2, "I wish Java could use goland"),
      (3, "Linear regression models are neat"),
      (4, "Logistic regression models are neat")
    )).toDF("id", "text")
    val test = MLUtils.idfFeatures(testDataFrame, numFeatures).select("features")

    val lrModel = LogisticRegressionModel.load(path)
    val result = lrModel.transform(test)

    result.show(false)

    spark.stop()
  }

}
