package com.spark.ml.classification

import com.spark.ml.util.{MLUtils, TrainingUtils}
import org.apache.spark.ml.classification.{DecisionTreeClassifier,DecisionTreeClassificationModel}
import org.apache.spark.sql.SparkSession

/**
  * Created by AnLei on 2017/11/28.
  *
  * DecisionTree
  */
object DecisionTree {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().master("local").appName(s"${this.getClass.getSimpleName}").getOrCreate()

    val path = "model/dt"
    val numFeatures = 10000

    /**
      * 训练集
      */
    val trainingDataFrame = spark.createDataFrame(TrainingUtils.trainingData).toDF("id", "text", "label")
    /**
      * 分词,向量化
      */
    val training = MLUtils.hashingFeatures(trainingDataFrame, numFeatures).select("label", "features")

    /**
      * 决策树模型
      */
    val dt = new DecisionTreeClassifier()
    /**
      * 保存模型
      */
    dt.fit(training).write.overwrite().save(path)

    /**
      * 测试集
      */
    val testDataFrame = spark.createDataFrame(TrainingUtils.testData).toDF("id", "text")
    /**
      * 分词,向量化
      */
    val test = MLUtils.hashingFeatures(testDataFrame, numFeatures).select("features")
    /**
      * 读取模型
      */
    val model = DecisionTreeClassificationModel.load(path)
    /**
      * 分类结果
      */
    val result = model.transform(test)

    result.show(false)

    spark.stop()
  }

}
