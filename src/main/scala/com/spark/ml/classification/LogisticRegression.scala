package com.spark.ml.classification

import com.spark.ml.util.{MLUtils, TrainingUtils}
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

    /**
      * 训练集
      */
    val trainingDataFrame = spark.createDataFrame(TrainingUtils.trainingData).toDF("id", "text", "label")
    /**
      * 分词,向量化,IDF
      */
    val training = MLUtils.idfFeatures(trainingDataFrame, numFeatures).select("label", "features")

    /**
      * 逻辑回归模型
      */
    val lr = new LogisticRegression()
      .setMaxIter(10)
      .setRegParam(0.001)
      .setFamily("multinomial") // binomial | multinomial
    /**
      * 保存模型
      */
    lr.fit(training).write.overwrite().save(path)

    /**
      * 测试集
      */
    val testDataFrame = spark.createDataFrame(TrainingUtils.testData).toDF("id", "text")
    /**
      * 分词,向量化,IDF
      */
    val test = MLUtils.idfFeatures(testDataFrame, numFeatures).select("features")
    /**
      * 读取模型
      */
    val model = LogisticRegressionModel.load(path)
    /**
      * 分类结果
      */
    val result = model.transform(test)

    result.show(false)

    spark.stop()
  }

}
