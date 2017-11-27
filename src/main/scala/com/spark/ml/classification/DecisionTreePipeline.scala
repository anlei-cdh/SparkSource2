package com.spark.ml.classification

import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.feature.{HashingTF, Tokenizer}
import org.apache.spark.sql.SparkSession

object DecisionTreePipeline {

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
    val hashingTF = new HashingTF().setInputCol(tokenizer.getOutputCol).setOutputCol("features").setNumFeatures(numFeatures)
    /**
      * TF-IDF 管道使用这个特征不准确
      */
    // val idf = new IDF().setInputCol(hashingTF.getOutputCol).setOutputCol("features")
    /**
      * 逻辑回归
      */
    val lr = new LogisticRegression().setMaxIter(10).setRegParam(0.001)
    /**
      * 管道
      */
    val pipeline = new Pipeline().setStages(Array(tokenizer, hashingTF, lr))
    pipeline.fit(trainingData).write.overwrite().save(path)

    val testData = spark.createDataFrame(Seq(
      (1, "Hi I'd like spark"),
      (2, "I wish Java could use goland"),
      (3, "Linear regression models are neat"),
      (4, "Logistic regression models are neat")
    )).toDF("id", "text")

    val pipelineModel = PipelineModel.load(path)
    val result = pipelineModel.transform(testData)
    result.show(false)

    spark.stop()
  }

}
