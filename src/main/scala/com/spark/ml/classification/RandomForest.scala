package com.spark.ml.classification

import com.spark.ml.util.{MLUtils, TrainingUtils}
import org.apache.spark.ml.classification.{RandomForestClassifier, RandomForestClassificationModel}
import org.apache.spark.sql.SparkSession

/**
  * Created by AnLei on 2017/11/28.
  *
  * RandomForest
  */
object RandomForest {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().master("local").appName(s"${this.getClass.getSimpleName}").getOrCreate()

    val path = "model/rf"
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
      * 随机森林模型
      */
    val rf = new RandomForestClassifier()
      .setNumTrees(50)
    /**
      * 保存模型
      */
    rf.fit(training).write.overwrite().save(path)

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
    val model = RandomForestClassificationModel.load(path)
    /**
      * 分类结果
      */
    val result = model.transform(test)

    result.show(false)

    spark.stop()
  }

}
