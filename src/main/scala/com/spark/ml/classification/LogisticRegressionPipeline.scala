package com.spark.ml.classification

import com.spark.ml.util.MLUtils
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.ml.classification.{LogisticRegression, LogisticRegressionModel}
import org.apache.spark.ml.feature.{HashingTF, IDF, Tokenizer}
import org.apache.spark.sql.SparkSession

object LogisticRegressionPipeline {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().master("local").appName(s"${this.getClass.getSimpleName}").getOrCreate()

    val path = "model/lr"
    val numFeatures = 10000

    val trainingData = spark.createDataFrame(Seq(
      (1, "Hi I heard about Spark", 0.0),
      (2, "I wish Java could use case classes", 0.0),
      (3, "Logistic regression models are neat", 1.0)
    )).toDF("id", "text","label")

    /**
      * 分词
      */
    val tokenizer = new Tokenizer().setInputCol("text").setOutputCol("words")
    /**
      * 向量化
      */
    val hashingTF = new HashingTF().setInputCol(tokenizer.getOutputCol).setOutputCol("rawFeatures").setNumFeatures(numFeatures)
    /**
      * TF-IDF
      */
    val idf = new IDF().setInputCol(hashingTF.getOutputCol).setOutputCol("features")
    /**
      * 逻辑回归
      */
    val lr = new LogisticRegression().setMaxIter(10).setRegParam(0.001)
    /**
      * 管道
      */
    val pipeline = new Pipeline().setStages(Array(tokenizer, hashingTF, idf, lr))
    pipeline.fit(trainingData).write.overwrite().save(path)

    val testData = spark.createDataFrame(Seq(
      (1, "Hi I'd like spark"),
      (2, "I wish Java could use goland"),
      (3, "Linear regression models are neat"),
      (4, "Logistic regression models are neat")
    )).toDF("id", "text")

    val lrModel = PipelineModel.load(path)
    val result = lrModel.transform(testData)
    result.show(false)

    spark.stop()
  }

}
