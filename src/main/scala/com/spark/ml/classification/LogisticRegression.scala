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

    val trainingData = spark.createDataFrame(Seq(
      (0.0, "Hi I heard about Spark"),
      (0.0, "I wish Java could use case classes"),
      (1.0, "Logistic regression models are neat")
    )).toDF("label", "sentence")
    val training = MLUtils.idfFeatures(trainingData).select("label", "features")

    val lr = new LogisticRegression()
      .setMaxIter(10)
      .setRegParam(0.3)
      .setElasticNetParam(0.8)
//    val lrModel = lr.fit(training)
//    lr.fit(training).save(path)

    val testData = spark.createDataFrame(Seq(
      (0.0, "Hi I'd like spark"),
      (0.0, "I wish Java could use goland"),
      (0.0, "Linear regression models are neat"),
      (0.0, "Logistic regression models are neat")
    )).toDF("label", "sentence")
    val test = MLUtils.idfFeatures(testData).select("features")

    val lrModel = LogisticRegressionModel.load(path)
    val result = lrModel.transform(test)
    result.show(false)

    spark.stop()
  }

}
